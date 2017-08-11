import sys, time
import pika
import yaml
import logging

#Determine pika logger level based on optional third argument
def parseLevel(arg):
    if arg.lower() == "debug":
        savedMessage = "Pika logging level set to DEBUG"
        return (savedMessage,logging.DEBUG)
    elif arg.lower() == "info":
        savedMessage = "Pika logging level set to INFO"
        return (savedMessage,logging.INFO)
    elif arg.lower() == "warning":
        savedMessage = "Pika logging level set to WARNING"
        return (savedMessage,logging.WARNING)
    elif arg.lower() == "error":
        savedMessage = "Pika logging level set to ERROR"
        return (savedMessage,logging.ERROR)
    elif arg.lower() == "critical":
        savedMessage = "Pika logging level set to CRITICAL"
        return (savedMessage,logging.CRITICAL)
    else:
        savedMessage = "Unrecognized Pika logging level input. Pika logging level set to ERROR"
        return (savedMessage,logging.ERROR)

#Initialize the logger to write on console and to the file.
def initialize_logger(level):
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to info.
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # create file handler and set level to error.
    handler = logging.FileHandler("createQLog.log","w",encoding=None, delay = "true")
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Setup the pika module logger to level determined earlier.
    logging.getLogger("pika").setLevel(level)


#Read the config from Config*.yaml file.
# Entry Parameter: configfile - YAML file with Rabbit queue configuration data.
# Exit  Return: tuple(queues) - list of queue configuration read from the configfile.
def readConfig(configfile):
    global connHost, connPort, connUseSSL, connSocketTimeout, connVHost
    global connUsername, connPassword, connPrefetchCount
    
    with open(configfile,'r') as ymlfile:
       cfg = yaml.load(ymlfile)

    try:
        connHost = cfg['conn']['connHost']
        connPort = cfg['conn']['connPort']
        connUseSSL = cfg['conn']['connUseSSL']
        connSocketTimeout = cfg['conn']['connSocketTimeout']
        connVHost = cfg['conn']['connVHost']
        connUsername = cfg['conn']['connUsername']
        connPassword = cfg['conn']['connPassword']
        connPrefetchCount = cfg['conn']['connPrefetchCount']
    except Exception as e:
        logging.error("Missing Connection config parameters. %s" % (e))
        sys.exit("Missing Connection Configs.")
    # Queues we are using:
    queues = cfg['queues']
    return (queues)

# Connect to the RabbitMQ Broker.
# Entry: Global connection parameters are set:
#   connHost, connPort, connUseSSL, connSocketTimeout, connVHost
#   connUsername, connPassword, connPrefetchCount
# Returns:  tuple (connection, channel).
def connect():
    global connHost, connPort, connUseSSL, connSocketTimeout, connVHost
    global connUsername, connPassword, connPrefetchCount
    vhost = connVHost
    if vhost == "/":
        vhost = "%2f"
        
    protocol = "amqp"
    if connUseSSL:
        protocol = "amqps"
    
    # Most connection parameters are put into the URL.
    url = "%s://%s:%s@%s:%d/%s" % (
        protocol, connUsername, connPassword, 
        connHost, connPort, vhost
    )
    params = pika.URLParameters(url)
    
    # If > 0, set the socketTimeout in seconds; on high-latency VPN
    # connections, you may need to set this to 3 or 4.
    if connSocketTimeout > 0:
        params.socket_timeout = connSocketTimeout
        
    logging.info("Connecting to RMQ host %s as user %s" % (connHost, connUsername))
    logging.debug("  " + url)
    
    # Connect; this call blocks until connected or an error occurs.
    conn = pika.BlockingConnection(params)
    channel = None
    if conn.is_open:
        logging.debug("Connected.")
        channel = conn.channel()
        if connPrefetchCount > 0:
            channel.basic_qos(prefetch_count=connPrefetchCount)
    return (conn, channel)    

    
# Declare the RMQ exchanges and queues that we will use.
# Entry parameters: channel - virtual connection, queues - list of queue configuration details
def declare(channel,queues):
    createCount = 0
    failedCount = 0
    validateCount = 0
    # We are using the default Direct exchange; it does not need to be declared.
    # Example: a durable fanout exchange:
    #channel.exchange_declare(exchange="psFanout", exchange_type="fanout", durable=True)
    for queue in queues:
    # A durable queue:
        try:
            #Checks to see if the Queue is already present.
           declareOk = channel.queue_declare(queue=queue['name'], passive='true')
            #if present validates the the existing queue parameters.
           declareOk = channel.queue_declare(queue=queue['name'], durable=queue['durable'], exclusive= queue['exclusive'],auto_delete= queue['auto_delete'],arguments= queue['arguments'])
           validateCount += 1
           logging.info("Queue %s has %d message(s)." % (queue['name'], declareOk.method.message_count))
        except Exception as e:
            if conn.is_open:
                channel = conn.channel()
                #If the Queue does not exist already, then create the Queue.
                if e.args[0] == 404:
                    (channel, createCount, failedCount) = createQueue(channel, queue, createCount, failedCount)
                else:
                    failedCount += 1
                    logging.error("Queue %s on validation got %s exception." % (queue['name'], e))
            
    logging.info("**************Results**************")
    logging.info("Total %d queues." % len(queues))
    logging.info("Declared %d queues." % createCount)
    logging.info("Validated %d queues." % validateCount)
    logging.info("Failed to declare/validate %d queues." % failedCount)
    logging.info("***********************************")

# Create Queue.
# Entry parameters: channel - virtual connection, queues - list of queue configuration details, createCount - no. of queues created, failedCount - no. of queues failed to declare/validate.
# Exit returns: tuple(channel, createCount, failedCount)
def createQueue(channel,queue,createCount,failedCount):
        try:
            declareOk = channel.queue_declare(queue=queue['name'],
                                              durable=queue['durable'],
                                              exclusive= queue['exclusive'],
                                              auto_delete= queue['auto_delete'],
                                              arguments= queue['arguments'])
            createCount += 1
            logging.info("Queue %s has %d message(s)." % (queue['name'],
                                                          declareOk.method.message_count))
        except Exception as e:
            logging.error("Queue %s on creation got %s exception." % (queue['name'], e))
            failedCount += 1
            if conn.is_open:
                logging.debug("After error, it is still Connected.")
                channel = conn.channel()
        return (channel,createCount,failedCount)


# Main Program:

##Used for debugging only##
#sys.argv = ["CreateRabbitQueues.py", "Configs/configRten.yml"]
###########################

# Find Pika logging level
level = logging.ERROR
savedMessage = "Default Pika logging level (ERROR) used"
if len(sys.argv) > 2:
    (savedMessage,level) = parseLevel(sys.argv[2])

#setup logger.
initialize_logger(level)
logging.info(savedMessage)

if len(sys.argv) < 2:
    logging.error("NOTE:   Expecting fileName as command line arguments. ")
    sys.exit("Missing fileName argument.")
queues = readConfig(sys.argv[1])

# Connect to the server.
(conn, channel) = connect()
if conn.is_open:
    # Declare the queues we will use.
    declare(channel,queues)

    

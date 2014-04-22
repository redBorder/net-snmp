/*
 * File       : snmptrapd_kafka
 * Author     : Eugenio Perez eupm90@gmail.com (based on Robert Story's snmptrapd_sql)
 *
 * Copyright © 2014 Eneo Tecnologia S.L. All rights reserved.
 * Use is subject to license terms specified in the COPYING file
 * distributed with the Net-SNMP package.
 *
 * This file implements a handler for snmptrapd which will cache incoming
 * traps and then write them to a Apache kafka queue, using Magnus Edenhill librdkafka.
 *
 */
#include <net-snmp/net-snmp-config.h>

#ifdef NETSNMP_USE_RDKAFKA

#if HAVE_STDLIB_H
#include <stdlib.h>
#endif
#if HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <stdio.h>
#if HAVE_STRING_H
#include <string.h>
#else
#include <strings.h>
#endif
#include <ctype.h>
#include <sys/types.h>
#if HAVE_WINSOCK_H
#include <winsock.h>
#else
#include <netinet/in.h>
#include <netdb.h>
#endif

#include <net-snmp/net-snmp-includes.h>
#include <net-snmp/agent/net-snmp-agent-includes.h>
#include "snmptrapd_handlers.h"
#include "snmptrapd_auth.h"
#include "snmptrapd_log.h"

/*
 * librdkafka includes
 */
#include "librdkafka/rdkafka.h"

/*
 * define a structure to hold all the file globals
 */
typedef struct netsnmp_kafka_globals_t {
    char        *brokers;       /* broker address (def=localhost) */
    char        *topic;         /* kafka topic to write to */
    uint16_t     port_num;      /* port number (built-in value) */

    rd_kafka_t *rk;
} netsnmp_kafka_globals;

static netsnmp_kafka_globals _kafka = {
    .brokers = NULL,
    .topic = NULL,
    .port_num = 9092,

    .rk = NULL,
};

/* FW */
int kafka_handler(netsnmp_pdu *pdu,netsnmp_transport *transport,netsnmp_trapd_handler *handler);

/*
 * register kafka related configuration tokens
 */
void
snmptrapd_register_kafka_configs( void )
{
    // @TODO ?
}

/*
 * convenience function to log kafka errors
 */
// @TODO
#if 0
static void
netsnmp_kafka_error(const char *message)
{
    u_int err = mysql_errno(_sql.conn);
    snmp_log(LOG_ERR, "%s\n", message);
    if (_sql.conn != NULL) {
#if MYSQL_VERSION_ID >= 40101
        snmp_log(LOG_ERR, "Error %u (%s): %s\n",
                 err, mysql_sqlstate(_sql.conn), mysql_error(_sql.conn));
#else
        snmp(LOG_ERR, "Error %u: %s\n",
             mysql_errno(_sql.conn), mysql_error(_sql.conn));
#endif
    }
    if (CR_SERVER_GONE_ERROR == err)
        netsnmp_sql_disconnected();
}
#endif

/*
 * sql cleanup function, called at exit
 */
static void
netsnmp_kafka_cleanup(void)
{
    DEBUGMSGTL(("kafka:handler", "called\n"));

    free(_kafka.brokers);
    free(_kafka.topic);

    /* Wait for messages to be delivered */
    rd_kafka_poll(_kafka.rk, 100);

    rd_kafka_destroy(_kafka.rk);
}

/**
 * Magnus Edenhill's message delivery report callback.
 * Called once for each message.
 */
static void msg_delivered (rd_kafka_t *rk,
void *payload, size_t len,
int error_code,
void *opaque, void *msg_opaque) {

    if (error_code){
        snmp_log(LOG_ERR, "kafka: Message delivery failed: %s\n", rd_kafka_err2str(error_code));
    }else if(snmp_get_do_debugging()){
        snmp_log(LOG_DEBUG, "kafka: Message delivered (%zd bytes): %*.*s\n", len,(int)len,(int)len,(char *)payload);
    }
}

/** one-time initialization for mysql */
int
netsnmp_kafka_init(void)
{
    netsnmp_trapd_handler *traph = NULL;
    rd_kafka_conf_t *conf = NULL;
    rd_kafka_topic_conf_t *topic_conf = NULL;
    char errstr[512];

    DEBUGMSGTL(("kafka:init","called\n"));

    /** load .my.cnf values */
    _kafka.brokers = strdup("pablo03");
    _kafka.topic = strdup("eugenio");

    if(_kafka.brokers == NULL){
        snmp_log(LOG_ERR, "kafka:No brokers defined\n");
        return -1;
    }

    if(_kafka.topic == NULL){
        snmp_log(LOG_ERR, "kafka:No topic defined\n");
        return -1;
    }

    conf = rd_kafka_conf_new();
    if(NULL==conf){
        snmp_log(LOG_ERR,"rd_kafka_conf_new() failed (out of memory?)\n");
        return -1;
    }

    rd_kafka_conf_set_dr_cb(conf, msg_delivered);

    topic_conf = rd_kafka_topic_conf_new();
    if(NULL==topic_conf){
        snmp_log(LOG_ERR,"rd_kafka_topic_conf_new() failed (out of memory?)\n");
        return -1;
    }

    _kafka.rk = rd_kafka_new (RD_KAFKA_PRODUCER,conf,errstr,sizeof(errstr));
    if (_kafka.rk == NULL) {
        snmp_log(LOG_ERR,"rd_kafka_new() failed: %s\n",errstr);
        return -1;
    }

    /** add handler */
    // @TODO search about PRE_HANDLER and alternatives
    traph = netsnmp_add_global_traphandler(NETSNMPTRAPD_PRE_HANDLER,kafka_handler);
    if (NULL == traph) {
        snmp_log(LOG_ERR, "Could not allocate kafka trap handler\n");
        return -1;
    }
    traph->authtypes = TRAP_AUTH_LOG;

    atexit(netsnmp_kafka_cleanup);
    return 0;
}

/*
 * kafka trap handler
 */
int
kafka_handler(netsnmp_pdu           *pdu,
              netsnmp_transport     *transport,
              netsnmp_trapd_handler *handler)
{
    // sql_buf     *sqlb;
    // int          old_format, rc;

    DEBUGMSGTL(("sql:handler", "called\n"));

    #if 0
    /** allocate a buffer to save data */
    sqlb = _sql_buf_get();
    if (NULL == sqlb) {
        snmp_log(LOG_ERR, "Could not allocate trap sql buffer\n");
        return syslog_handler( pdu, transport, handler );
    }

    /** save OID output format and change to numeric */
    old_format = netsnmp_ds_get_int(NETSNMP_DS_LIBRARY_ID,
                                    NETSNMP_DS_LIB_OID_OUTPUT_FORMAT);
    netsnmp_ds_set_int(NETSNMP_DS_LIBRARY_ID, NETSNMP_DS_LIB_OID_OUTPUT_FORMAT,
                       NETSNMP_OID_OUTPUT_NUMERIC);


    rc = _sql_save_trap_info(sqlb, pdu, transport);
    rc = _sql_save_varbind_info(sqlb, pdu);

    /** restore previous OID output format */
    netsnmp_ds_set_int(NETSNMP_DS_LIBRARY_ID, NETSNMP_DS_LIB_OID_OUTPUT_FORMAT,
                       old_format);

    // @TODO
    /** insert into queue */
    rc = CONTAINER_INSERT(_sql.queue, sqlb);
    if(rc) {
        snmp_log(LOG_ERR, "Could not log queue sql trap buffer\n");
        _sql_log(sqlb, NULL);
        _sql_buf_free(sqlb, 0);
        return -1;
    }

    /** save queue if size is > max */
    if (CONTAINER_SIZE(_sql.queue) >= _sql.queue_max)
        _sql_process_queue(0,NULL);
    #endif

    return 0;
}

#endif /* NETSNMP_USE_KAFKA */

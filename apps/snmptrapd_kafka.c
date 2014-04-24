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

#include <errno.h>

#include <net-snmp/net-snmp-includes.h>
#include <net-snmp/agent/net-snmp-agent-includes.h>
#include "snmptrapd_handlers.h"
#include "snmptrapd_auth.h"
#include "snmptrapd_log.h"

#include "librdkafka/rdkafka.h"

#define STRBUFFER_MIN_SIZE 2048
#define STRBUFFER_FACTOR 2
#define STRBUFFER_SIZE_MAX ((size_t)-1)

#define jsonp_malloc malloc
#define jsonp_free   free

#define max(a,b) (((a) > (b)) ? (a) : (b))
#define min(a,b) (((a) < (b)) ? (a) : (b))

/* Obtained from jansson strbuffer library */
typedef struct {
    char *value;
    size_t length; /* bytes used */
    size_t size; /* bytes allocated */
} strbuffer_t;

static int strbuffer_init(strbuffer_t *strbuff)
{
    strbuff->size = STRBUFFER_MIN_SIZE;
    strbuff->length = 0;

    strbuff->value = malloc(strbuff->size);
    if(!strbuff->value)
        return -1;

    /* initialize to empty */
    strbuff->value[0] = '\0';
    return 0;
}

static strbuffer_t *strbuffer_new(void){
    strbuffer_t *b = calloc(1,sizeof(*b));
    if(0==strbuffer_init(b)){
        return b;
    }else{
        free(b);
        return NULL;
    }
}

static void strbuffer_close(strbuffer_t *strbuff)
{
    if(strbuff->value)
        jsonp_free(strbuff->value);

    strbuff->size = 0;
    strbuff->length = 0;
    strbuff->value = NULL;
}

static char *strbuffer_steal_value(strbuffer_t *strbuff)
{
    char *result = strbuff->value;
    strbuff->value = NULL;
    return result;
}

static int strbuffer_append_bytes(strbuffer_t *strbuff, const char *data, size_t size)
{
    if(size >= strbuff->size - strbuff->length)
    {
        size_t new_size;
        char *new_value;

        /* avoid integer overflow */
        if (strbuff->size > STRBUFFER_SIZE_MAX / STRBUFFER_FACTOR
            || size > STRBUFFER_SIZE_MAX - 1
            || strbuff->length > STRBUFFER_SIZE_MAX - 1 - size)
            return -1;

        new_size = max(strbuff->size * STRBUFFER_FACTOR,
                       strbuff->length + size + 1);

        new_value = jsonp_malloc(new_size);
        if(!new_value)
            return -1;

        memcpy(new_value, strbuff->value, strbuff->length);

        jsonp_free(strbuff->value);
        strbuff->value = new_value;
        strbuff->size = new_size;
    }

    memcpy(strbuff->value + strbuff->length, data, size);
    strbuff->length += size;
    strbuff->value[strbuff->length] = '\0';

    return 0;
}

static int strbuffer_append(strbuffer_t *strbuff, const char *string){
    return strbuffer_append_bytes(strbuff, string, strlen(string));
}


/*
 * define a structure to hold all the file globals
 */
typedef struct netsnmp_kafka_globals_t {
    char        *brokers;       /* broker address (def=localhost) */
    char        *topic;         /* kafka topic to write to */
    uint16_t     port_num;      /* port number (built-in value) */

    rd_kafka_t *rk;
    rd_kafka_topic_t *rkt;
} netsnmp_kafka_globals;

static netsnmp_kafka_globals _kafka = {
    .brokers = NULL,
    .topic = NULL,
    .port_num = 9092,

    .rk  = NULL,
    .rkt = NULL,
};

/* FW */
int kafka_handler(netsnmp_pdu *pdu,netsnmp_transport *transport,netsnmp_trapd_handler *handler);

/*
 * register kafka related configuration tokens
 */
static void
snmptrapd_register_kafka_configs( void )
{
    // @TODO
}

/**
* Magnus Edenhill kafkacat producer.
*
* Produces a single message, retries on queue congestion, and
* exits hard on error.
*/
static void produce (void *buf, size_t len, int msgflags) {
    int retried = 0;
    /* Produce message: keep trying until it succeeds. */

    do {
        if (rd_kafka_produce(_kafka.rkt, RD_KAFKA_PARTITION_UA, msgflags,
                             buf, len, NULL, 0, NULL) != -1) {
            break;
        }

        const rd_kafka_resp_err_t rkerr = rd_kafka_errno2err(errno);

        if (rkerr != RD_KAFKA_RESP_ERR__QUEUE_FULL){
            // @TODO use rd_kafka_err2str
            snmp_log(LOG_ERR,"Failed to produce message (%zd bytes): %s",len,rd_kafka_err2str(rkerr));
            free(buf);
            break;
        }

        /* Internal queue full, sleep to allow
        * messages to be produced/time out
        * before trying again. */
        rd_kafka_poll(_kafka.rk, 5);
        if(retried){
            snmp_log(LOG_ERR,"Cannot produce message.");
            free(buf);
            break;
        }
    } while (1);

    /* Poll for delivery reports, errors, etc. */
    rd_kafka_poll(_kafka.rk, 0);
}

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

    _kafka.rk = rd_kafka_new (RD_KAFKA_PRODUCER,conf,errstr,sizeof(errstr));
    if (_kafka.rk == NULL) {
        snmp_log(LOG_ERR,"rd_kafka_new() failed: %s\n",errstr);
        return -1;
    }

    if (rd_kafka_brokers_add(_kafka.rk, _kafka.brokers) == 0) {
        snmp_log(LOG_ERR, "kafka:No valid brokers specified\n");
        return -1;
    }

    topic_conf = rd_kafka_topic_conf_new();
    if(NULL==topic_conf){
        snmp_log(LOG_ERR,"rd_kafka_topic_conf_new() failed (out of memory?)\n");
        return -1;
    }

    _kafka.rkt = rd_kafka_topic_new(_kafka.rk, _kafka.topic, topic_conf);
    if (NULL == _kafka.rkt){
        snmp_log(LOG_ERR,"Failed to create topic %s: %s\n", 
            _kafka.topic, rd_kafka_err2str(rd_kafka_errno2err(errno)));
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

static char* _itoa10(uint64_t value, char* result, size_t bufsize) {
    char *ptr = result+bufsize;
    uint64_t tmp_value;

    *--ptr = '\0';
    do {
        tmp_value = value;
        value /= 10;
        *--ptr = "zyxwvutsrqponmlkjihgfedcba9876543210123456789abcdefghijklmnopqrstuvwxyz" [35 + (tmp_value - value * 10)];
    } while ( value );


    if (tmp_value < 0) *--ptr = '-';
    return ptr;
}

static void print_string(strbuffer_t *buffer,const char *str){
    strbuffer_append(buffer,"\"");
    strbuffer_append(buffer,str);
    strbuffer_append(buffer,"\"");
}

static void print_attr_name(strbuffer_t *buffer,const char *attr_name){
    print_string(buffer,attr_name);
    strbuffer_append(buffer,":");
}

static int host2strbuffer(strbuffer_t *buffer,const char *attribute_name,netsnmp_pdu *pdu,netsnmp_transport *transport){
    const size_t start_buffer_length = buffer->length;

    char *str_buf = calloc(2048,sizeof(char));
    if(NULL==str_buf){
        snmp_log(LOG_ERR,"kafka: Cannot allocate buffer memory memory");
        return 0;
    }
    size_t tmp_size = 0;

    size_t copied = 0;
    print_attr_name(buffer,attribute_name);
    strbuffer_append(buffer,str_buf);
    const int rc = realloc_format_trap((u_char **)&str_buf,&tmp_size,&copied,1,"%B",pdu,transport);

    if(rc == 1 && str_buf){
        // all ok
        strbuffer_append(buffer,"\"");
        strbuffer_append_bytes(buffer,str_buf,copied);
        strbuffer_append(buffer,"\"");
    }else{
        snmp_log(LOG_ERR,"kafka:Cannot print host: buffer=%p,str_buf=%p,rc=%d",buffer,str_buf,rc);
        buffer->length = start_buffer_length;
        buffer->value[buffer->length] = '\0';
    }

    free(str_buf);
    return rc;
}

struct oid_s{
    oid *trap_oid;
    int trap_oid_len;
};

struct oid_s extract_oid(const netsnmp_pdu *pdu){
    oid         *trap_oid;
    int          trap_oid_len;
    struct oid_s ret_oid = {NULL, 0};

    if (pdu->command == SNMP_MSG_TRAP) {
        /*
         * convert a v1 trap to a v2 varbind
         */
        if (pdu->trap_type == SNMP_TRAP_ENTERPRISESPECIFIC) {
            oid tmp_oid[MAX_OID_LEN];
            trap_oid_len = pdu->enterprise_length;
            memcpy(tmp_oid, pdu->enterprise, sizeof(oid) * trap_oid_len);
            if (tmp_oid[trap_oid_len - 1] != 0)
                tmp_oid[trap_oid_len++] = 0;
            tmp_oid[trap_oid_len++] = pdu->specific_type;
            trap_oid = tmp_oid;
        } else {
            static oid trapoids[] = { 1, 3, 6, 1, 6, 3, 1, 1, 5, 0 };
            trapoids[9] = pdu->trap_type + 1;
            trap_oid = trapoids;
            trap_oid_len = OID_LENGTH(trapoids);
        }
    } else {
        netsnmp_variable_list *vars = pdu->variables;
        if (vars && vars->next_variable) {
            trap_oid_len = vars->next_variable->val_len / sizeof(oid);
            trap_oid = vars->next_variable->val.objid;
        } else {
            static oid null_oid[] = { 0, 0 };
            trap_oid_len = OID_LENGTH(null_oid);
            trap_oid = null_oid;
        }
    }

    ret_oid.trap_oid = trap_oid;
    ret_oid.trap_oid_len = trap_oid_len;
    return ret_oid;
}

static void oid2strbuffer0(strbuffer_t *buffer,oid *trap_oid,const size_t trap_oid_len){
    char *strbuffer = NULL;

    size_t tmp_size = 0;
    size_t buf_oid_len_t = 0;
    int    oid_overflow = 0;
    
    netsnmp_sprint_realloc_objid_tree((u_char**)&strbuffer,&tmp_size,
                                      &buf_oid_len_t, 1, &oid_overflow,
                                      trap_oid, trap_oid_len);
    
    strbuffer_append_bytes(buffer,strbuffer,buf_oid_len_t);

    if (oid_overflow)
        snmp_log(LOG_WARNING,"OID truncated in sql buffer\n");

    free(strbuffer);
}

static void oid2strbuffer(strbuffer_t *buffer, const char *attribute_name, netsnmp_pdu *pdu){
    print_attr_name(buffer,attribute_name);

    const struct oid_s soid = extract_oid(pdu);
    strbuffer_append(buffer,"\"");
    oid2strbuffer0(buffer,soid.trap_oid,soid.trap_oid_len);
    strbuffer_append(buffer,"\"");
}

/*
 * Append the pdu and transport information to a json strbuffer
 */
static int
pdu2strbuffer(strbuffer_t       *buffer,
              netsnmp_pdu       *pdu,
              netsnmp_transport *transport)
{
    static const size_t AUXBUFSIZE = 128;
    char aux[AUXBUFSIZE];

    strbuffer_append(buffer,"{");

    strbuffer_append(buffer,"\"timestamp\":\"");
    strbuffer_append(buffer,_itoa10(time(NULL),aux,AUXBUFSIZE));
    strbuffer_append(buffer,"\",");

    host2strbuffer(buffer,"host",pdu,transport);
    strbuffer_append(buffer,",");

    oid2strbuffer(buffer,"oid",pdu);

    strbuffer_append(buffer,"}");

    return 0;
}

/*
 * Transform the pdu and transport information to a string
 */
static char *
pdu2buffer(netsnmp_pdu           *pdu,
           netsnmp_transport     *transport)
{
    DEBUGMSGTL(("kafka:handler", "called\n"));
    
    char *ret_buffer = NULL;
    strbuffer_t *buffer = strbuffer_new();

    if(buffer){
        pdu2strbuffer(buffer,pdu,transport);
        ret_buffer = strbuffer_steal_value(buffer);
        strbuffer_close(buffer);
    }

    return ret_buffer;
}

/*
 * kafka trap handler
 */
int
kafka_handler(netsnmp_pdu           *pdu,
              netsnmp_transport     *transport,
              netsnmp_trapd_handler *handler)
{
    char *buffer = NULL;

    DEBUGMSGTL(("kafka:handler", "called\n"));

    buffer = pdu2buffer(pdu,transport);
    if(NULL==buffer){
        snmp_log(LOG_ERR, "Could not allocate trap sql buffer\n");
        return syslog_handler( pdu, transport, handler );
    }

    // snmp_log(LOG_DEBUG, "kafka:Produced %s\n",buffer);
    produce(buffer,strlen(buffer),RD_KAFKA_MSG_F_FREE);

    #if 0
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

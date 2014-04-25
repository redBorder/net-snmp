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

#define DONT_SEND_EMPTY_MESSAGES

/* Inspired jansson strbuffer library */
typedef struct {
    char *value;
    size_t buf_out; /* bytes used */
    size_t buf_len; /* bytes allocated */
} strbuffer_t;

static int strbuffer_init(strbuffer_t *strbuff)
{
    strbuff->buf_len = STRBUFFER_MIN_SIZE;
    strbuff->buf_out = 0;

    strbuff->value = calloc(STRBUFFER_MIN_SIZE,strbuff->buf_len);
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
        free(strbuff->value);

    strbuff->buf_len = 0;
    strbuff->buf_out = 0;
    strbuff->value = NULL;
}

static char *strbuffer_steal_value(strbuffer_t *strbuff)
{
    char *result = strbuff->value;
    strbuff->value = NULL;
    return result;
}

static int strbuffer_append(strbuffer_t *strbuff, const char *data){
    return snmp_strcat((u_char **)&strbuff->value, &strbuff->buf_len, &strbuff->buf_out, 1/*allow_realloc*/, (const u_char *)data);
}

static int 
strbuffer_append_bytes(strbuffer_t *strbuff, const char *data, size_t size)
{
    (void)size;
    return strbuffer_append(strbuff,data);
}


static void strbuffer_append_escape_newlines(strbuffer_t *buffer,const char *data){
    const char *newline = "\n";
    const char *cursor = data;
    char *nlchar = strpbrk(cursor,newline);
    while(cursor){
        if(nlchar){
            strbuffer_append_bytes(buffer,cursor,nlchar - cursor);
            strbuffer_append_bytes(buffer,"\\n",strlen("\\n"));
            cursor = nlchar+1;
            if(*cursor != '\0')
                nlchar = strpbrk(cursor,newline);
            else
                cursor = NULL;
        }else if(*cursor != '\0'){
            strbuffer_append(buffer,cursor);
            cursor = NULL;
        }
    }
}

#if 0
void test_strbuffer_append_escape_newlines(){
    strbuffer_t *str1 = strbuffer_new();
    strbuffer_append_escape_newlines(str1,"NOLINEBREAKS");
    puts(strbuffer_steal_value(str1));

    strbuffer_t *str2 = strbuffer_new();
    strbuffer_append_escape_newlines(str2,"MANY\nLINE\nBREAKS");
    puts(strbuffer_steal_value(str2));

    strbuffer_t *str3 = strbuffer_new();
    strbuffer_append_escape_newlines(str3,"ENDING\nWITH\nLINE\nBREAKS\n");
    puts(strbuffer_steal_value(str3));

}
#endif

#if 0
void test_format_functions(void){
    char *buf = NULL;
    size_t buf_len = 0;
    size_t out_len = 0;
    const int allow_realloc = 1;

    snmp_strcat((u_char **)&buf,&buf_len,&out_len,allow_realloc,(const u_char *)"Hello");
    printf("After add hello:\n");
    printf("buf:\t%p\n",buf);
    printf("buf_len:\t%lu\n",buf_len);
    printf("out_len:\t%lu\n",out_len);
    printf("allow_realloc:\t%d\n\n",allow_realloc);

    snmp_strcat((u_char **)&buf,&buf_len,&out_len,allow_realloc,(const u_char *)" world");
    printf("After add world:\n");
    printf("buf:\t%p\n",buf);
    printf("buf_len:\t%lu\n",buf_len);
    printf("out_len:\t%lu\n",out_len);
    printf("allow_realloc:\t%d\n\n",allow_realloc);

}
#endif

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

static void
_parse_kafka_topic(const char *token,char *cptr){
    _kafka.topic = strdup(cptr);
    snmp_log(LOG_DEBUG,"kafka: topic now %s\n",_kafka.topic);
}

static void
_parse_kafka_brokers(const char *token,char *cptr){
    _kafka.brokers = strdup(cptr);
    snmp_log(LOG_DEBUG,"kafka:brokers: the brokers will be requested to %s\n",_kafka.topic);
}

/*
 * register kafka related configuration tokens
 */
void
snmptrapd_register_kafka_configs( void )
{
    register_config_handler("snmptrapd", "kafkaTopic",
                            _parse_kafka_topic, NULL, "string");
    register_config_handler("snmptrapd", "kafkaBrokers",
                            _parse_kafka_brokers, NULL, "string");
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
 * kafka cleanup function, called at exit
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

/* Avoid print TYPE: value format. Instead, it prints directly the value */
static int set_netsnmp_quick_print(void){
    const int rc = netsnmp_ds_set_boolean(NETSNMP_DS_LIBRARY_ID, NETSNMP_DS_LIB_QUICK_PRINT,1);
    if(rc != SNMPERR_SUCCESS)
        snmp_log(LOG_ERR,"snmp values will not be printed in quick format\n");
    return rc;
}

static int set_netsnmp_escape_quotes(void){
    const int rc = netsnmp_ds_set_boolean(NETSNMP_DS_LIBRARY_ID, NETSNMP_DS_LIB_ESCAPE_QUOTES,1);
    if(rc != SNMPERR_SUCCESS)
        snmp_log(LOG_ERR,"quotes will not be escaped\n");
    return rc;
}

static int set_infty_hex_output_length(void){
    const int rc = netsnmp_ds_set_int(NETSNMP_DS_LIBRARY_ID, NETSNMP_DS_LIB_HEX_OUTPUT_LENGTH, 2048);
    if(rc != SNMPERR_SUCCESS)
        snmp_log(LOG_ERR,"quotes will not be escaped\n");
    return rc;
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

    set_netsnmp_quick_print();
    set_netsnmp_escape_quotes();
    set_infty_hex_output_length();

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

static int strbuffer_format_trap(strbuffer_t *buffer,const char *attribute_name,const char *format, netsnmp_pdu *pdu,netsnmp_transport *transport){
    const size_t start_buffer_length = buffer->buf_out;

    print_attr_name(buffer,attribute_name);
    strbuffer_append(buffer,"\"");
    const int rc = realloc_format_trap((u_char **)&buffer->value,&buffer->buf_len,&buffer->buf_out,1,format,pdu,transport);

    if(rc == 1){
        // all ok
        strbuffer_append(buffer,"\"");
    }else{
        snmp_log(LOG_ERR,"kafka:Cannot print host: buffer=%p,rc=%d",buffer,rc);
        buffer->buf_out = start_buffer_length;
        buffer->value[buffer->buf_out] = '\0';
    }

    return rc;
}

static int host2strbuffer(strbuffer_t *buffer,const char *attribute_name,netsnmp_pdu *pdu,netsnmp_transport *transport){
    return strbuffer_format_trap(buffer,attribute_name,"%B", pdu,transport);
}

struct oid_s{
    oid *trap_oid;
    int trap_oid_len;
};

// @TODO not thread safe. Pass tmp_oid in a parameter, and use it instead of trapoids
struct oid_s extract_oid(const netsnmp_pdu *pdu){
    oid         *trap_oid;
    int          trap_oid_len;
    struct oid_s ret_oid = {NULL, 0};

    if (pdu->command == SNMP_MSG_TRAP) {
        /*
         * convert a v1 trap to a v2 varbind
         */
        if (pdu->trap_type == SNMP_TRAP_ENTERPRISESPECIFIC) {
            static oid tmp_oid[MAX_OID_LEN];
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
    int    oid_overflow = 0;
    
    netsnmp_sprint_realloc_objid_tree((u_char**)&buffer->value,&buffer->buf_len,
                                      &buffer->buf_out, 1, &oid_overflow,
                                      trap_oid, trap_oid_len);

    if (oid_overflow)
        snmp_log(LOG_WARNING,"OID truncated in sql buffer\n");
}

static void oid2strbuffer(strbuffer_t *buffer, const char *attribute_name, netsnmp_pdu *pdu){
    print_attr_name(buffer,attribute_name);

    const struct oid_s soid = extract_oid(pdu);
    strbuffer_append(buffer,"\"");
    oid2strbuffer0(buffer,soid.trap_oid,soid.trap_oid_len);
    strbuffer_append(buffer,"\"");
}

static void number2buffer(strbuffer_t *buffer,const int number){
    char buf[128];
    strbuffer_append(buffer,_itoa10(number,buf,128));
}

static void reqid2buffer(strbuffer_t *buffer,const char *attribute_name, const netsnmp_pdu *pdu){
    print_attr_name(buffer,attribute_name);
    number2buffer(buffer,pdu->reqid);
}

static void version2buffer(strbuffer_t *buffer,const char *attr_name,const netsnmp_pdu *pdu){
    print_attr_name(buffer,attr_name);
    number2buffer(buffer,pdu->version+1);
}

static void security_model2buffer(strbuffer_t *buffer,const char *attr_name,const netsnmp_pdu *pdu){
    print_attr_name(buffer,attr_name);
    number2buffer(buffer,pdu->securityModel);
}

static void command2buffer(strbuffer_t *buffer,const char *attr_name,const netsnmp_pdu *pdu){
    print_attr_name(buffer,attr_name);
    number2buffer(buffer,pdu->command-159);
}

static void community2buffer(strbuffer_t *buffer,const char *attribute_name,netsnmp_pdu *pdu,netsnmp_transport *transport){
    strbuffer_format_trap(buffer,attribute_name,"%u", pdu,transport);
}

static void transport2buffer(strbuffer_t *buffer,const char *attr_name,netsnmp_pdu *pdu,netsnmp_transport *transport){
    print_attr_name(buffer,attr_name);
    const size_t initial_length = buffer->buf_out;

    char * str_transport = transport->f_fmtaddr(transport, pdu->transport_data,pdu->transport_data_length);
    if(transport){
        strbuffer_append(buffer,"\"");
        strbuffer_append(buffer,str_transport);
        strbuffer_append(buffer,"\"");
    }else{
        snmp_log(LOG_ERR,"Cannot get transport\n");
        buffer->buf_out = initial_length;
        buffer->value[buffer->buf_out] = '\0';
    }
    SNMP_FREE(str_transport);
}

static int trapinfo2strbuffer(strbuffer_t *buffer,
                              netsnmp_pdu       *pdu,
                              netsnmp_transport *transport)
{
    static const size_t AUXBUFSIZE = 128;
    char aux[AUXBUFSIZE];

    strbuffer_append(buffer,"\"timestamp\":\"");
    strbuffer_append(buffer,_itoa10(time(NULL),aux,AUXBUFSIZE));
    strbuffer_append(buffer,"\",");


    host2strbuffer(buffer,"host",pdu,transport);
    strbuffer_append(buffer,",");
    oid2strbuffer(buffer,"oid",pdu);

    strbuffer_append(buffer,",");
    reqid2buffer(buffer,"reqid",pdu);
    strbuffer_append(buffer,",");
    version2buffer(buffer,"version",pdu);
    strbuffer_append(buffer,",");
    command2buffer(buffer,"command",pdu);
    strbuffer_append(buffer,",");
    community2buffer(buffer,"community",pdu,transport);
    strbuffer_append(buffer,",");
    transport2buffer(buffer,"transport",pdu,transport);

    strbuffer_append(buffer,",");
    security_model2buffer(buffer,"security_model",pdu);

    return 0;
}

static int have_to_add_quotes(const int type){
    switch(type){
    case ASN_IPADDRESS:
    case ASN_OBJECT_ID:
    case ASN_TIMETICKS:
        return 1;
    default:
        return 0;
    };
}

static size_t print_var_oid_as_json_key(strbuffer_t *buffer,netsnmp_variable_list *var){
    const size_t initial_size = buffer->buf_out;
    
    int overflow = 0;
    strbuffer_append(buffer,"\"");
    netsnmp_sprint_realloc_objid_tree((u_char**)&buffer->value, &buffer->buf_len,
                                          &buffer->buf_out,
                                          1, &overflow, var->name,
                                          var->name_length);
    strbuffer_append(buffer,"\":");

    if(overflow)
        snmp_log(LOG_WARNING,"OID truncated in var2strbuffer");

    return buffer->buf_out - initial_size;
}

static size_t print_var_value_as_json_value(strbuffer_t *buffer,netsnmp_variable_list *var){
    const size_t initial_size = buffer->buf_out;
    const int _have_to_add_quotes = have_to_add_quotes(var->type);

    if(_have_to_add_quotes)
        strbuffer_append(buffer,"\"");
    const int value_rc = sprint_realloc_by_type((u_char**)&buffer->value, &buffer->buf_len,
                               &buffer->buf_out, 1, var, NULL, NULL, NULL);
    if(_have_to_add_quotes)
        strbuffer_append(buffer,"\"");

    if(value_rc!=1){
        snmp_log(LOG_ERR,"Something went wrong with sprintf_by_tipe (out of memory?).");
        return value_rc;
    }

#ifdef DONT_SEND_EMPTY_MESSAGES
    if(buffer->value[initial_size] == '\0' || strcmp(&buffer->value[initial_size],"\"\"") == 0){
        snmp_log(LOG_DEBUG,"Discarding empty line.\n");
        return 0;
    }
#endif

    return buffer->buf_out - initial_size;
}

static int var2strbuffer(strbuffer_t *buffer,netsnmp_variable_list *var){
    const size_t key_len   = print_var_oid_as_json_key(buffer,var);
    const size_t value_len = print_var_value_as_json_value(buffer,var);

#ifdef DONT_SEND_EMPTY_MESSAGES
    if(value_len == 0)
        return 0;
#endif

    return key_len + value_len;
}

static int varbind2strbuffer(strbuffer_t *buffer,netsnmp_pdu *pdu){
    netsnmp_variable_list *var = pdu->variables;
    while(var){
        const size_t initial_length = buffer->buf_out;
        strbuffer_append(buffer,",");
        
        const int bytes_writted = var2strbuffer(buffer,var);
        if(bytes_writted <= 0){
            buffer->buf_out = initial_length;
            buffer->value[buffer->buf_out] = '\0';
        }


        var = var->next_variable;
    }

    return 0;
}

/*
 * Append the pdu and transport information to a json strbuffer
 */
static int
pdu2strbuffer(strbuffer_t       *buffer,
              netsnmp_pdu       *pdu,
              netsnmp_transport *transport)
{
    strbuffer_append(buffer,"{");

    trapinfo2strbuffer(buffer,pdu,transport);
    varbind2strbuffer(buffer,pdu);

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

    return 0;
}

#endif /* NETSNMP_USE_KAFKA */

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
} netsnmp_kafka_globals;

static netsnmp_kafka_globals _kafka = {
    .brokers = NULL,
    .topic = NULL,
    .port_num = 9092
};

/*
 * log traps as text, or binary blobs?
 */
#define NETSNMP_KAFKA_TRAP_VALUE_TEXT 1

/*
 * We will be using prepared statements for performance reasons. This
 * requires a sql bind structure for each cell to be inserted in the
 * database. We will be using 2 global static structures to bind to,
 * and a netsnmp container to store the necessary data until it is
 * written to the database. Fixed size buffers are also used to
 * simplify memory management.
 */
/** enums for the trap fields to be bound */
enum{
    TBIND_DATE = 0,           /* time received */
    TBIND_HOST,               /* src ip */
    TBIND_USER,               /* auth/user information */
    TBIND_TYPE,               /* pdu type */
    TBIND_VER,                /* snmp version */
    TBIND_REQID,              /* request id */
    TBIND_OID,                /* trap OID */
    TBIND_TRANSPORT,          /* transport */
    TBIND_SECURITY_MODEL,     /* security model */
    TBIND_v3_MSGID,           /* v3 msg id */
    TBIND_v3_SECURITY_LEVEL,  /* security level */
    TBIND_v3_CONTEXT_NAME,    /* context */
    TBIND_v3_CONTEXT_ENGINE,  /* context engine id */
    TBIND_v3_SECURITY_NAME,   /* security name */
    TBIND_v3_SECURITY_ENGINE, /* security engine id */
    TBIND_MAX
};

/** enums for the varbind fields to be bound */
enum {
    VBIND_ID = 0,             /* trap_id */
    VBIND_OID,                /* varbind oid */
    VBIND_TYPE,               /* varbind type */
    VBIND_VAL,                /* varbind value */
    VBIND_MAX
};

/** buffer struct for varbind data */
typedef struct sql_vb_buf_t {

    char      *oid;
    u_long     oid_len;

    u_char    *val;
    u_long     val_len;

    u_int16_t  type;

} sql_vb_buf;

/** buffer struct for trap data */
typedef struct sql_buf_t {
    char      *host;
    u_long     host_len;

    char      *oid;
    u_long     oid_len;

    char      *user;
    u_long     user_len;

    time_t time;
    u_int16_t  version, type;
    u_int32_t  reqid;

    char      *transport;
    u_long     transport_len;

    u_int16_t  security_level, security_model;
    u_int32_t  msgid;

    char      *context;
    u_long     context_len;

    char      *context_engine;
    u_long     context_engine_len;

    char      *security_name;
    u_long     security_name_len;

    char      *security_engine;
    u_long     security_engine_len;

    netsnmp_container *varbinds;

    char       logged;
} sql_buf;

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
    // @TODO
}

/** one-time initialization for mysql */
int
netsnmp_kafka_init(void)
{
    netsnmp_trapd_handler *traph = NULL;

    DEBUGMSGTL(("kafka:init","called\n"));

    /** load .my.cnf values */
    _kafka.brokers = strdup("localhost");

    #if 0
    // @TODO
    _sql.conn = mysql_init (NULL);
    if (_sql.conn == NULL) {
        netsnmp_kafka_error("mysql_init() failed (out of memory?)");
        return -1;
    }
    #endif

    /** add handler */
    // @TODO search about PRE_HANDLER and alternatives
    traph = netsnmp_add_global_traphandler(NETSNMPTRAPD_PRE_HANDLER,kafka_handler);
    if (1 || NULL == traph) {
        snmp_log(LOG_ERR, "Could not allocate kafka trap handler\n");
        return -1;
    }
    traph->authtypes = TRAP_AUTH_LOG;

    atexit(netsnmp_kafka_cleanup);
    return 0;
}

/*
 * log CSV version of trap.
 * dontcare param is there so this function can be passed directly
 * to CONTAINER_FOR_EACH.
 */
static void
_kafka_log(sql_buf *sqlb, void* dontcare)
{
    netsnmp_iterator     *it;
    sql_vb_buf           *sqlvb;

    if ((NULL == sqlb) || sqlb->logged)
        return;

    /*
     * log trap info
     * nothing done to protect against data insertion attacks with
     * respect to bad data (commas, newlines, etc)
     */
    snmp_log(LOG_ERR,
             "trap:%lu,%s,%d,%d,%d,%s,%s,%d,%d,%d,%s,%s,%s,%s\n",
             sqlb->time,
             sqlb->user,
             sqlb->type, sqlb->version, sqlb->reqid, sqlb->oid,
             sqlb->transport, sqlb->security_model, sqlb->msgid,
             sqlb->security_level, sqlb->context,
             sqlb->context_engine, sqlb->security_name,
             sqlb->security_engine);

    sqlb->logged = 1; /* prevent multiple logging */

    it = CONTAINER_ITERATOR(sqlb->varbinds);
    if (NULL == it) {
        snmp_log(LOG_ERR,
                 "error creating iterator; incomplete trap logged\n");
        return;
    }

    /** log varbind info */
    for( sqlvb = ITERATOR_FIRST(it); sqlvb; sqlvb = ITERATOR_NEXT(it)) {
#ifdef NETSNMP_KAFKA_TRAP_VALUE_TEXT
        snmp_log(LOG_ERR,"varbind:%s,%s\n", sqlvb->oid, sqlvb->val);
#else
        char *hex;
        int len = binary_to_hex(sqlvb->val, sqlvb->val_len, &hex);
        if (hex) {
            snmp_log(LOG_ERR,"varbind:%d,%s,%s\n", sqlvb->oid, hex);
            free(hex);
        }
        else {
            snmp_log(LOG_ERR,"malloc failed for varbind hex value\n");
            snmp_log(LOG_ERR,"varbind:%s,\n", sqlvb->oid);
        }
#endif
    }
    ITERATOR_RELEASE(it);
   
}

/*
 * free a buffer
 * dontcare param is there so this function can be passed directly
 * to CONTAINER_FOR_EACH.
 */
static void
_sql_vb_buf_free(sql_vb_buf *sqlvb, void* dontcare)
{
    if (NULL == sqlvb)
        return;

    SNMP_FREE(sqlvb->oid);
    SNMP_FREE(sqlvb->val);

    free(sqlvb);
}

/*
 * free a buffer
 * dontcare param is there so this function can be passed directly
 * to CONTAINER_FOR_EACH.
 */
static void
_sql_buf_free(sql_buf *sqlb, void* dontcare)
{
    if (NULL == sqlb)
        return;

    /** do varbinds first */
    if (sqlb->varbinds) {
        CONTAINER_CLEAR(sqlb->varbinds,
                        (netsnmp_container_obj_func*)_sql_vb_buf_free, NULL);
        CONTAINER_FREE(sqlb->varbinds);
    }

    SNMP_FREE(sqlb->host);
    SNMP_FREE(sqlb->oid);
    SNMP_FREE(sqlb->user);

    SNMP_FREE(sqlb->context);
    SNMP_FREE(sqlb->security_name);
    SNMP_FREE(sqlb->context_engine);
    SNMP_FREE(sqlb->security_engine);
    SNMP_FREE(sqlb->transport);

    free(sqlb);
}

/*
 * allocate buffer to store trap and varbinds
 */
static sql_buf *
_sql_buf_get(void)
{
    sql_buf *sqlb;

    /** buffer for trap info */
    sqlb = SNMP_MALLOC_TYPEDEF(sql_buf);
    if (NULL == sqlb)
        return NULL;
    
    /** fifo for varbinds */
    sqlb->varbinds = netsnmp_container_find("fifo");
    if (NULL == sqlb->varbinds) {
        free(sqlb);
        return NULL;
    }

    return sqlb;
}

/*
 * save info from incoming trap
 *
 * return 0 on success, anything else is an error
 */
static int
_sql_save_trap_info(sql_buf *sqlb, netsnmp_pdu  *pdu,
                    netsnmp_transport     *transport)
{
    static oid   trapoids[] = { 1, 3, 6, 1, 6, 3, 1, 1, 5, 0 };
    oid         *trap_oid, tmp_oid[MAX_OID_LEN];
    size_t       tmp_size;
    size_t       buf_host_len_t, buf_oid_len_t, buf_user_len_t;
    int          oid_overflow, rc, trap_oid_len;
    netsnmp_variable_list *vars;

    if ((NULL == sqlb) || (NULL == pdu) || (NULL == transport))
        return -1;

    DEBUGMSGTL(("sql:queue", "queueing incoming trap\n"));

    /** time */
    time(&sqlb->time);
    
    /** host name */
    buf_host_len_t = 0;
    tmp_size = sizeof(sqlb->host);
    rc = realloc_format_trap((u_char**)&sqlb->host, &tmp_size,
                             &buf_host_len_t, 1, "%B", pdu, transport);
    sqlb->host_len = buf_host_len_t;

    /* snmpTrapOID */
    if (pdu->command == SNMP_MSG_TRAP) {
        /*
         * convert a v1 trap to a v2 varbind
         */
        if (pdu->trap_type == SNMP_TRAP_ENTERPRISESPECIFIC) {
            trap_oid_len = pdu->enterprise_length;
            memcpy(tmp_oid, pdu->enterprise, sizeof(oid) * trap_oid_len);
            if (tmp_oid[trap_oid_len - 1] != 0)
                tmp_oid[trap_oid_len++] = 0;
            tmp_oid[trap_oid_len++] = pdu->specific_type;
            trap_oid = tmp_oid;
        } else {
            trapoids[9] = pdu->trap_type + 1;
            trap_oid = trapoids;
            trap_oid_len = OID_LENGTH(trapoids);
        }
    }
    else {
        vars = pdu->variables;
        if (vars && vars->next_variable) {
            trap_oid_len = vars->next_variable->val_len / sizeof(oid);
            trap_oid = vars->next_variable->val.objid;
        }
        else {
            static oid null_oid[] = { 0, 0 };
            trap_oid_len = OID_LENGTH(null_oid);
            trap_oid = null_oid;
        }
    }
    tmp_size = 0;
    buf_oid_len_t = oid_overflow = 0;
    netsnmp_sprint_realloc_objid_tree((u_char**)&sqlb->oid,&tmp_size,
                                      &buf_oid_len_t, 1, &oid_overflow,
                                      trap_oid, trap_oid_len);
    sqlb->oid_len = buf_oid_len_t;
    if (oid_overflow)
        snmp_log(LOG_WARNING,"OID truncated in sql buffer\n");

    /** request id */
    sqlb->reqid = pdu->reqid;

    /** version (convert to 1 based, for sql enum) */
    sqlb->version = pdu->version + 1;

    /** command type (convert to 1 based, for sql enum) */
    sqlb->type = pdu->command - 159;

    /** community string/user name */
    tmp_size = 0;
    buf_user_len_t = 0;
    rc = realloc_format_trap((u_char**)&sqlb->user, &tmp_size,
                             &buf_user_len_t, 1, "%u", pdu, transport);
    sqlb->user_len = buf_user_len_t;

    /** transport */
    sqlb->transport = transport->f_fmtaddr(transport, pdu->transport_data,
                                           pdu->transport_data_length);

    /** security model */
    sqlb->security_model = pdu->securityModel;

    if ((SNMP_MP_MODEL_SNMPv3+1) == sqlb->version) {

        sqlb->msgid = pdu->msgid;
        sqlb->security_level = pdu->securityLevel;

        if (pdu->contextName) {
            sqlb->context = netsnmp_strdup_and_null((u_char*)pdu->contextName,
                                                    pdu->contextNameLen);
            sqlb->context_len = pdu->contextNameLen;
        }
        if (pdu->contextEngineID) {
            sqlb->context_engine_len = 
                binary_to_hex(pdu->contextEngineID, pdu->contextEngineIDLen,
                              &sqlb->context_engine);
        }

        if (pdu->securityName) {
            sqlb->security_name =
                netsnmp_strdup_and_null((u_char*)pdu->securityName,
                                        pdu->securityNameLen);
            sqlb->security_name_len = pdu->securityNameLen;
        }
        if (pdu->securityEngineID) {
            sqlb->security_engine_len = 
                binary_to_hex(pdu->securityEngineID, pdu->securityEngineIDLen,
                              &sqlb->security_engine);
        }
    }

    return 0;
}

/*
 * save varbind info from incoming trap
 *
 * return 0 on success, anything else is an error
 */
static int
_sql_save_varbind_info(sql_buf *sqlb, netsnmp_pdu  *pdu)
{
    netsnmp_variable_list *var;
    sql_vb_buf       *sqlvb;
    size_t            tmp_size, buf_oid_len_t;
    int               oid_overflow, rc;
#ifdef NETSNMP_KAFKA_TRAP_VALUE_TEXT
    size_t            buf_val_len_t;
#endif

    if ((NULL == sqlb) || (NULL == pdu))
        return -1;

    var = pdu->variables;
    while(var) {
        sqlvb = SNMP_MALLOC_TYPEDEF(sql_vb_buf);
        if (NULL == sqlvb)
            break;

        /** OID */
        tmp_size = 0;
        buf_oid_len_t = oid_overflow = 0;
        netsnmp_sprint_realloc_objid_tree((u_char**)&sqlvb->oid, &tmp_size,
                                          &buf_oid_len_t,
                                          1, &oid_overflow, var->name,
                                          var->name_length);
        sqlvb->oid_len = buf_oid_len_t;
        if (oid_overflow)
            snmp_log(LOG_WARNING,"OID truncated in sql insert\n");
        
        /** type */
        if (var->type > ASN_OBJECT_ID)
            /** convert application types to sql enum */
            sqlvb->type = ASN_OBJECT_ID + 1 + (var->type & ~ASN_APPLICATION);
        else
            sqlvb->type = var->type;

        /** value */
#ifdef NETSNMP_KAFKA_TRAP_VALUE_TEXT
        tmp_size = 0;
        buf_val_len_t = 0;
        sprint_realloc_by_type((u_char**)&sqlvb->val, &tmp_size,
                               &buf_val_len_t, 1, var, 0, 0, 0);
        sqlvb->val_len = buf_val_len_t;
#else
        memdup(&sqlvb->val, var->val.string, var->val_len);
        sqlvb->val_len = var->val_len;
#endif

        var = var->next_variable;

        /** insert into container */
        rc = CONTAINER_INSERT(sqlb->varbinds,sqlvb);
        if(rc)
            snmp_log(LOG_ERR, "couldn't insert varbind into trap container\n");
    }

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
    sql_buf     *sqlb;
    int          old_format, rc;

    DEBUGMSGTL(("sql:handler", "called\n"));

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

    #if 0
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

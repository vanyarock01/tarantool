#include <math.h>

#include <module.h>
#include <msgpuck.h>

#include "config.h"
#include "router_api.h"

struct box_function_ctx {
	struct port *port;
};

typedef struct box_function_ctx box_function_ctx_t;

enum {
	FUNC_NAME_LEN_MAX = 256
};

static void
parseReplicaCfg(const char **msgpack, struct ReplicaCfg *replica)
{
	assert(mp_typeof(**msgpack) == MP_STR);
	replica->uuid = mp_decode_str(msgpack, &replica->uuid_len);
	uint32_t replica_map = mp_decode_map(msgpack);
	say_info("replica map %d", replica_map);
	for (uint32_t i = 0; i < replica_map; ++i) {
		assert(mp_typeof(**msgpack) == MP_STR);
		uint32_t str_len = 0;
		const char *key = mp_decode_str(msgpack, &str_len);
		if (str_len == 3 && strncmp(key, "uri", 3) == 0) {
			replica->uri =
				mp_decode_str(msgpack, &replica->uri_len);
			continue;
		}
		if (str_len == 4 && strncmp(key, "name", 4) == 0) {
			replica->name =
				mp_decode_str(msgpack, &replica->name_len);
			continue;
		}
		if (str_len == 6 && strncmp(key, "master", 5) == 0) {
			replica->is_master =
				mp_decode_bool(msgpack);
			continue;
		}
		say_error("Unknown replica cfg key %.*s",
			  str_len, key);
	}
	say_info("Decoded replica: name=%.*s, uri=%.*s, uuid=%.*s, master=%d",
		 replica->name_len, replica->name,
		 replica->uri_len, replica->uri,
		 replica->uuid_len, replica->uuid,
		 replica->is_master);
}

static int
parseReplicaSetCfg(const char **msgpack, struct ReplicaSetCfg *rs)
{
	assert(mp_typeof(**msgpack) == MP_STR);
	rs->uuid = mp_decode_str(msgpack, &rs->uuid_len);
	assert(mp_typeof(**msgpack) == MP_MAP);
	uint32_t map_size = mp_decode_map(msgpack);
	assert(mp_typeof(**msgpack) == MP_STR);
	uint32_t str_len;
	const char *replicas = mp_decode_str(msgpack, &str_len);
	assert(str_len == 8);/* "replicas" */
	assert(mp_typeof(**msgpack) == MP_MAP);
	rs->replica_count = mp_decode_map(msgpack);
	say_info("replica count %u", rs->replica_count);
	rs->replicas = calloc(rs->replica_count, sizeof(struct ReplicaCfg));
	if (rs->replicas == NULL) {
		box_error_set(__FILE__, __LINE__, ER_PROC_C,
			      "Failed to allocate memory!");
		return -1;
	}
	for (uint32_t j = 0; j < rs->replica_count; ++j)
		parseReplicaCfg(msgpack, &rs->replicas[j]);
	return 0;
}

static int
parseShardingCfg(const char **msgpack, struct Config *cfg)
{
	assert(mp_typeof(**msgpack) == MP_MAP);
	cfg->replicaSets_count = mp_decode_map(msgpack);
	say_info("NUMBER OF REPLICASETS %u", cfg->replicaSets_count);
	cfg->replicaSets = calloc(cfg->replicaSets_count, sizeof(struct ReplicaSetCfg));
	if (cfg->replicaSets == NULL) {
		box_error_set(__FILE__, __LINE__, ER_PROC_C,
			      "Failed to allocate memory!");
		return -1;
	}
	for (uint32_t i = 0; i < cfg->replicaSets_count; ++i) {
		say_info("REPLICASET %u", i);
		if (parseReplicaSetCfg(msgpack, &cfg->replicaSets[i]) != 0)
			return -1;
	}
	return 0;
}

/** C API entry points. */
int
router_cfg(box_function_ctx_t *ctx, const char *args,
	   const char *args_end)
{
	struct Config cfg;
	memset(&cfg, 0, sizeof(struct Config));
	say_info("----- router_cfg() -----");
	assert(mp_typeof(*args) == MP_ARRAY);
	uint32_t arr_size = mp_decode_array(&args);
	assert(mp_typeof(*args) == MP_MAP);
	uint32_t map_size = mp_decode_map(&args);
	for (uint32_t i = 0; i < map_size; ++i) {
		assert(mp_typeof(*args) == MP_STR);
		uint32_t str_len;
		const char *key = mp_decode_str(&args, &str_len);
		if (str_len == 8 && strncmp(key, "sharding", 8) == 0) {
			if (parseShardingCfg(&args, &cfg) != 0) {
				config_free(&cfg);
				return -1;
			}
			continue;
		}
		if (str_len == 12 && strncmp(key, "bucket_count", 12) == 0) {
			assert(mp_typeof(*args) == MP_UINT);
			cfg.bucket_count = mp_decode_uint(&args);
			continue;
		}
	}
	router_build(&cfg);
	config_free(&cfg);
	say_info("Router has been configured!");
	return 0;
}

/**
 * Arguments format:
 * @param bucket_id Integer value representing bucket id.
 * @param mode String value representing call mode (read or write).
 * @param function String value representing function to call on storage.
 * @param args Table of arguments to be passed to @function on storage.
 *
 * On client side call of this function looks like:
 * conn:call('storagec.vshard_router_callrw', {bucket_id, func, args})
 *
 * Where conn is casual netbox connection.
 */
int
router_callrw(box_function_ctx_t *ctx, const char *args, const char *args_end)
{
	(void) ctx;
	if (mp_typeof(*args) != MP_ARRAY) {
		say_error("Wrong arguments format: array is expected");
		return -1;
	}
	uint32_t arr_size = mp_decode_array(&args);
	if (arr_size != 4) {
		say_error("Wrong arguments format: 4 arguments expected, "\
			  "but got %d", arr_size);
		return -1;
	}

	if (mp_typeof(*args) != MP_UINT) {
		say_error("Wrong arguments format: bucket_id is expected "\
			  "to be integer");
		return -1;
	}
	uint64_t bucket_id = mp_decode_uint(&args);

	if (router_call(bucket_id, args, args_end, arr_size, ctx) != 0) {
		say_error("Router call failed!");
		return -1;
	}
	return 0;
}

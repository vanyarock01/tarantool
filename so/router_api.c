#include "module.h"

#include "router_api.h"
#include "Router.hpp"

#include <vector>

int
router_call(int bucket_id, const char *args, const char *args_end,
	    size_t args_count, box_function_ctx_t *ctx)
{
	std::vector<box_tuple_t *> tuples;
	Router &router = StaticRouterHolder::getRouter();
	if (router.call(bucket_id, args, args_end, args_count, tuples) != 0) {
		return -1;
	}
	for (auto t : tuples) {
		if (box_return_tuple(ctx, t) != 0) {
			return -1;
		}
	}
	return 0;
}

void
router_build(struct Config *cfg)
{
	Router &router = StaticRouterHolder::getRouter();
	router.reset();
	for (uint32_t i = 0; i < cfg->replicaSets_count; ++i) {
		struct ReplicaSetCfg *rs = &cfg->replicaSets[i];
		if (router.addReplicaSet(rs) != 0) {
			say_error("Failed to add replicaset %.*s",
				  rs->uuid_len, rs->uuid);
		}
	}
	say_info("Set bucket count=%u", cfg->bucket_count);
	router.setBucketCount(cfg->bucket_count);
	router.connectAll();
}

#pragma once

#include "config.h"

#ifdef __cplusplus
extern "C" {
#endif

void
router_build(struct Config *cfg);

int
router_call(int bucket_id, const char *args, const char *args_end,
	    size_t args_count, box_function_ctx_t *ctx);

#ifdef __cplusplus
}
#endif

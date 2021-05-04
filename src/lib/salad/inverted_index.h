#ifndef TARANTOOL_LIB_SALAD_INVERTED_INDEX_H_INCLUDED
#define TARANTOOL_LIB_SALAD_INVERTED_INDEX_H_INCLUDED

#include <string.h>
#include "inverted_list.h"

#define BPS_TREE_NAME inverted_index_tree
#define BPS_TREE_BLOCK_SIZE (512)
#define BPS_TREE_EXTENT_SIZE (16*1024)
#define BPS_TREE_COMPARE(a, b, ctx) inverted_list_compare(a, b)
#define BPS_TREE_COMPARE_KEY(a, b, ctx) strcmp(a, b)
#define BPS_TREE_IS_IDENTICAL(a, b) inverted_list_is_equal(a, b)
#define BPS_TREE_NAMESPACE DEFAULT

#define bps_tree_arg_t struct key_def *
#define bps_tree_elem_t struct inverted_list *
#define bps_tree_key_t char *

#include "salad/bps_tree.h"

struct inverted_index {
    inverted_index_tree *tree;
};

#endif
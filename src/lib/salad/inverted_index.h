#ifndef TARANTOOL_LIB_SALAD_INVERTED_INDEX_H_INCLUDED
#define TARANTOOL_LIB_SALAD_INVERTED_INDEX_H_INCLUDED

#include <string.h>
#include <stdlib.h>

// TODO: use custom tarantool memory allocator

typedef struct tuple_list {
    void **data;
    size_t len;
    size_t capacity;
} tuple_list_t;

static tuple_list_t *
tuple_list_create()
{
    tuple_list_t *lst = (tuple_list_t *) malloc(sizeof(tuple_list_t));
    lst->len = 0;
    lst->capacity = 1;
    lst->data = (void **) malloc(sizeof(void *));
    return lst;
}

static void
tuple_list_destroy(tuple_list_t *lst)
{
    free(lst->data);
    free(lst);
}

static int
tuple_list_insert(tuple_list_t *lst, void *tuple)
{
    if (lst->len == lst->capacity) {
        lst->capacity *= 2;
        lst->data = (void **) realloc(lst->data, lst->capacity * sizeof(void *));
        if (lst->data == NULL)
            return -1; // something wrong
    }
    lst->data[lst->len] = tuple;
    lst->len++;

    return 0;
}

static int
tuple_list_eq(tuple_list_t *a, tuple_list_t *b)
{
    if (a->len != b->len)
        return 0;

    return memcmp(a->data, b->data, a->len) == 0;
}

typedef char * token_t;
typedef struct inverted_tree_elem {
    token_t key;
    tuple_list_t *lst;
} tree_elem_t;

#define BPS_TREE_NAME inverted_index_tree
#define BPS_TREE_BLOCK_SIZE (512)
#define BPS_TREE_EXTENT_SIZE (16*1024)
#define BPS_TREE_COMPARE(a, b, ctx) strcmp((&a)->key, (&b)->key)
#define BPS_TREE_COMPARE_KEY(a, b, ctx) strcmp((&a)->key, b)
#define BPS_TREE_IS_IDENTICAL(a, b) strcmp((&a)->key, (&b)->key) && tuple_list_eq((&a)->lst, (&b)->lst)

#define bps_tree_arg_t int // struct key_def *
#define bps_tree_key_t token_t
#define bps_tree_elem_t tree_elem_t

#include "bps_tree.h"

// toy allocators with context for local tests
int extents_count = 0;

static void *
extent_alloc(void *ctx)
{
    int *p_extents_count = (int *)ctx;
    assert(p_extents_count == &extents_count);
    ++*p_extents_count;
    return malloc(BPS_TREE_EXTENT_SIZE);
}

static void
extent_free(void *ctx, void *extent)
{
    int *p_extents_count = (int *)ctx;
    assert(p_extents_count == &extents_count);
    --*p_extents_count;
    free(extent);
}

typedef struct inverted_index {
    inverted_index_tree *tree;
} inverted_index_t;

inverted_index_t *
inverted_index_create()
{
    inverted_index_t *idx = (inverted_index_t *) malloc(sizeof(inverted_index_t));
    idx->tree = (inverted_index_tree *) malloc(sizeof(inverted_index_tree));

    inverted_index_tree_create(idx->tree, 0, extent_alloc, extent_free, &extents_count);
    return idx;
}

void
inverted_index_destroy(inverted_index_t *idx)
{
    inverted_index_tree_iterator it = inverted_index_tree_iterator_first(idx->tree);
    for (int i = 0; i < inverted_index_tree_size(idx->tree); i++) {
        tree_elem_t *elem = inverted_index_tree_iterator_get_elem(idx->tree, &it);
        tuple_list_destroy(elem->lst);
        inverted_index_tree_iterator_next(idx->tree, &it);
    }
    inverted_index_tree_destroy(idx->tree);
    free(idx->tree);
    free(idx);
}

int
inverted_index_insert(inverted_index_t *idx, token_t token, void *tuple)
{
    tree_elem_t *res = inverted_index_tree_find(idx->tree, token);
    if (res != NULL)
        return tuple_list_insert((&res[0])->lst, tuple);

    tuple_list_t *lst = tuple_list_create();
    if (lst == NULL)
        return -1; // Something bad

    int rc = tuple_list_insert(lst, tuple);
    if (rc != 0)
        return rc;

    tree_elem_t elem = {
            token,
            lst
    };
    return inverted_index_tree_insert(idx->tree, elem, NULL, NULL);
}

tuple_list_t *
inverted_index_get(inverted_index_t *idx, token_t token)
{
    tree_elem_t *res = inverted_index_tree_find(idx->tree, token);
    if (res != NULL) {
        return res->lst;
    } else {
        return NULL;
    }
}

#endif
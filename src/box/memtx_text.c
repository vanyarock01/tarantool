#include "memtx_text.h"
#include "memtx_engine.h"
#include "space.h"
#include "schema.h"
#include "tuple.h"

#include <salad/inverted_index.h>


struct memtx_text_index {
    struct index base;
    inverted_index_t *data;
};

//static void
//memtx_text_index_begin_build(struct index *base)
//{
//    struct memtx_text_index *index = (struct memtx_text_index *)base;
//    assert(inverted_index_size(&(index->data)) == 0);
//    (void)index;
//}

//static z_address*
//extract_zaddress(struct tuple *tuple, struct mempool *pool,
//                 const struct memtx_zcurve_index *index)
//{
//    uint32_t key_size;
//    struct key_def *key_def = index->base.def->key_def;
//    const char* key = tuple_extract_key(tuple, key_def,
//                                        MULTIKEY_NONE, &key_size);
//    mp_decode_array(&key);
//    return mp_decode_key(pool, key, key_def->part_count, index);
//}

//char *
//parse_token_from_tuple(struct tuple *tuple, const struct memtx_text_index *index)
//{
//    uint32_t key_size;
//    struct key_def *key_def = index->base.def->key_def;
//    const char* key = tuple_extract_key(tuple, key_def,
//                                        MULTIKEY_NONE, &key_size);
//
//}
//
//static int
//memtx_text_index_build_next(struct index *base, struct tuple *tuple)
//{
//    struct memtx_text_index *index = (struct memtx_text_index *)base;
//    if (index->data == NULL) {
//        index->data = inverted_index_create();
//    }
//}

//static int
//memtx_zcurve_index_build_next(struct index *base, struct tuple *tuple)
//{
//    struct memtx_zcurve_index *index = (struct memtx_zcurve_index *)base;
//    if (index->build_array == NULL) {
//        index->build_array = malloc(MEMTX_EXTENT_SIZE);
//        if (index->build_array == NULL) {
//            diag_set(OutOfMemory, MEMTX_EXTENT_SIZE,
//                     "memtx_zcurve_index", "build_next");
//            return -1;
//        }
//        index->build_array_alloc_size =
//                MEMTX_EXTENT_SIZE / sizeof(index->build_array[0]);
//    }
//    assert(index->build_array_size <= index->build_array_alloc_size);
//    if (index->build_array_size == index->build_array_alloc_size) {
//        index->build_array_alloc_size = index->build_array_alloc_size +
//                                        index->build_array_alloc_size / 2;
//        struct memtx_zcurve_data *tmp =
//                realloc(index->build_array,
//                        index->build_array_alloc_size * sizeof(*tmp));
//        if (tmp == NULL) {
//            diag_set(OutOfMemory, index->build_array_alloc_size *
//                                  sizeof(*tmp), "memtx_zcurve_index", "build_next");
//            return -1;
//        }
//        index->build_array = tmp;
//    }
//    struct memtx_zcurve_data *elem =
//            &index->build_array[index->build_array_size++];
//    elem->tuple = tuple;
//    elem->z_address = extract_zaddress(tuple, &index->bit_array_pool,
//                                       index);
//    return 0;
//}

static ssize_t
memtx_text_index_size(struct index *base)
{
    struct memtx_text_index *index = (struct memtx_text_index *)base;
    return inverted_index_size(index->data);
}

static void
memtx_text_index_destroy(struct index *base)
{
    struct memtx_text_index *index = (struct memtx_text_index *)base;
    inverted_index_destroy(index->data);
    free(index);
}

static const struct index_vtab memtx_text_index_vtab = {
        /* .destroy = */ memtx_text_index_destroy,
        /* .commit_create = */ generic_index_commit_create,
        /* .abort_create = */ generic_index_abort_create,
        /* .commit_modify = */ generic_index_commit_modify,
        /* .commit_drop = */ generic_index_commit_drop,
        /* .update_def = */ generic_index_update_def,
        /* .depends_on_pk = */ generic_index_depends_on_pk,
        /* .def_change_requires_rebuild = */ generic_index_def_change_requires_rebuild,
        /* .size = */ memtx_text_index_size,
        /* .bsize = */ generic_index_bsize,
        /* .min = */ generic_index_min,
        /* .max = */ generic_index_max,
        /* .random = */ generic_index_random,
        /* .count = */ generic_index_count,
        /* .get = */ generic_index_get,
        /* .replace = */ generic_index_replace,
        /* .create_iterator = */ generic_index_create_iterator,
        /* .create_snapshot_iterator = */
                         generic_index_create_snapshot_iterator,
        /* .stat = */ generic_index_stat,
        /* .compact = */ generic_index_compact,
        /* .reset_stat = */ generic_index_reset_stat,
        /* .begin_build = */ generic_index_begin_build,
        /* .reserve = */ generic_index_reserve,
        /* .build_next = */ generic_index_build_next,
        /* .end_build = */ generic_index_end_build,
};

struct index *
memtx_text_index_new(struct memtx_engine *memtx, struct index_def *def)
{
    struct memtx_text_index *index =
            (struct memtx_text_index *)malloc(sizeof(*index));

    if (index == NULL) {
        diag_set(OutOfMemory, sizeof(*index), "malloc", "struct memtx_text_index");
        return NULL;
    }

    if (index_create(&index->base, (struct engine *)memtx,
                     &memtx_text_index_vtab, def) != 0) {
        free(index);
        return NULL;
    }

    // create index
    index->data = inverted_index_create(
            memtx_index_extent_alloc, memtx_index_extent_free, memtx);

    if (index->data == NULL) {
        free(index);
        return NULL;
    }
    return &index->base;
}

#ifndef TARANTOOL_LIB_SALAD_INVERTED_LIST_H_INCLUDED
#define TARANTOOL_LIB_SALAD_INVERTED_LIST_H_INCLUDED

// #include <small/...> TODO: use custom allocator
// The simplest possible list for storing pointers to tarantool tuples

#include <string.h>

typedef void* data_ptr;
typedef struct inverted_list t_inverted_list;

struct inverted_list {
    data_ptr *tuple_vector;
    size_t len;
    size_t capacity;
};

t_inverted_list *
inverted_list_create()
{
    t_inverted_list *lst = (t_inverted_list *) malloc(sizeof(t_inverted_list));
    lst->len = 0;
    lst->capacity = 1;
    lst->tuple_vector = (data_ptr *) malloc(sizeof(data_ptr) * lst->capacity);

    return lst;
}

void
inverted_list_free(t_inverted_list *lst)
{
    free(lst->tuple_vector);
    lst->tuple_vector = NULL;
}

void
inverted_list_insert(t_inverted_list *lst, data_ptr tuple)
{
    if (lst->len == lst->capacity) {
        lst->tuple_vector = (data_ptr *) realloc(lst->tuple_vector, sizeof(data_ptr) * lst->capacity * 2);
        lst->capacity *= 2;
    }

    lst->tuple_vector[lst->len] = tuple;
    lst->len++;
}

int
inverted_list_compare(t_inverted_list *lst_a, t_inverted_list *lst_b)
{
    if (lst_a->len < lst_b->len) {
        return -1;
    } else if (lst_a->len > lst_b->len) {
        return 1;
    } else {
        return 0;
    }
}

int inverted_list_is_equal(t_inverted_list *lst_a, t_inverted_list *lst_b)
{
    if (lst_a->len != lst_b->len) {
        return 0;
    }
    return memcmp(lst_a->tuple_vector, lst_b->tuple_vector, lst_a->len) == 0;
}

#endif
/*
 * Interface for the No Hot Spot Non-Blocking Skip List
 * operations.
 *
 * Author: Ian Dick, 2013.
 */

#ifndef NOHOTSPOT_OPS_H_
#define NOHOTSPOT_OPS_H_

#include "skiplist.h"

enum sl_optype {
        CONTAINS,
        DELETE,
        INSERT,
        SCAN,
};
typedef enum sl_optype sl_optype_t;

int sl_do_operation(long *steps, set_t *set, sl_optype_t optype, sl_key_t key, val_t val);

/* these are macros instead of functions to improve performance */
#define sl_contains(steps, a, b) sl_do_operation((steps), (a), CONTAINS, (b), NULL);
#define sl_delete(steps, a, b) sl_do_operation((steps), (a), DELETE, (b), NULL);
#define sl_insert(steps, a, b, c) sl_do_operation((steps), (a), INSERT, (b), (c));
// Note that the range must keep valid before this function returns
#define sl_scan(steps, a, start_key, range) sl_do_operation((steps), (a), SCAN, (start_key), (void *)&(range))

#endif /* NOHOTSPOT_OPS_H_ */

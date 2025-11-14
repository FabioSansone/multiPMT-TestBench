#ifndef RC_H
#define RC_H

#include <stdint.h>

volatile uint32_t *open_rc();
uint32_t rc_read(volatile uint32_t *map, int addr);
int rc_write(volatile uint32_t *map, int addr, uint32_t value);
void close_rc();

#endif

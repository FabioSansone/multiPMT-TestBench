#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include "runcontrol_acq.h"

#define UIO_DEVICE "/dev/uio0"
#define UIO_MAP_SIZE  0x10000

static int fd = -1;
static volatile uint32_t *regs = NULL;

int maxRegisterAddress = 50;

int checkRegBoundary(int addr){
    return (addr >= 0 && addr <= maxRegisterAddress);
}

volatile uint32_t *open_rc() {

    fd = open(UIO_DEVICE, O_RDWR);
    if (fd < 0){
        perror("Errore apertura UIO");
        return NULL;
    }

    regs = mmap(NULL, UIO_MAP_SIZE,
                PROT_READ | PROT_WRITE,
                MAP_SHARED, fd, 0);

    if (regs == MAP_FAILED){
        perror("mmap failed");
        close(fd);
        fd = -1;
        return NULL;
    }

    return regs;
}


uint32_t rc_read(volatile uint32_t *map, int addr){
    if (!checkRegBoundary(addr)){
        fprintf(stderr, "Address out of range\n");
        return 0;
    }
    return map[addr];
}

int rc_write(volatile uint32_t *map, int addr, uint32_t value){
    if (!checkRegBoundary(addr)){
        fprintf(stderr, "Address out of range\n");
        return -1;
    }
    map[addr] = value;
    return 0;
}

void close_rc(){
    if (regs != NULL){
        munmap((void*)regs, UIO_MAP_SIZE);
        regs = NULL;
    }
    if (fd >= 0){
        close(fd);
        fd = -1;
    }
}








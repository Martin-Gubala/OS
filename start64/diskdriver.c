/* 2888769g
COMPSCI4011 – Assessed Exercise
This is my own work*/

#include <pthread.h>
#include <stdlib.h>
#include "diskdriver.h"
#include "freesectordescriptorstore.h"
#include "BoundedBuffer.h"
#include "diskdevice.h" 

static DiskDevice *g_dd;
static FreeSectorDescriptorStore *g_fsds;
static BoundedBuffer *g_write_queue;
static BoundedBuffer *g_read_queue;  
static BoundedBuffer *g_read_results;
static pthread_t g_writer_tid, g_reader_tid;
static pthread_mutex_t g_voucher_lock = PTHREAD_MUTEX_INITIALIZER;
static BoundedBuffer *g_voucher_pool;


void init_disk_driver(DiskDevice *dd, void *mem_start, unsigned long mem_length,FreeSectorDescriptorStore **fsds);


void blocking_write_sector(SectorDescriptor *sd, Voucher **v);
int nonblocking_write_sector(SectorDescriptor *sd, Voucher **v);


void blocking_read_sector(SectorDescriptor *sd, Voucher **v);
int nonblocking_read_sector(SectorDescriptor *sd, Voucher **v);


int redeem_voucher(Voucher *v, SectorDescriptor **sd);

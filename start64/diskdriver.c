/* 2888769g
COMPSCI4011 – Assessed Exercise
This is my own work*/

#include <pthread.h>
#include <stdio.h>
#include <assert.h>
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


void init_disk_driver(DiskDevice *dd, void *mem_start, unsigned long mem_length,FreeSectorDescriptorStore **fsds){
    
    FreeSectorDescriptorStore *store;
    g_dd = dd;
    g_fsds=store;
    
    store = create_fsds();
    create_free_sector_descriptors(store, mem_start, mem_length);
    *fsds = store;

    g_write_queue = createBB(16);
    g_read_queue = createBB(16);

    g_free_requests = createBB(16);
    g_free_vouchers = createBB(16);

    pthread_create(&g_writer_tid, NULL, writer_thread_func, NULL); //still undefind
    pthread_create(&g_reader_tid, NULL, reader_thread_func, NULL);
}
void blocking_write_sector(SectorDescriptor *sd, Voucher **v){
    DriverRequest *req = aquire_reqest_blocking();
    Voucher *new_v = acquire_voucher_blocking();

    req->sd = sd;
    req->voucher = new_v;
    req->is_read = 0;

    *v = new_v;
    blockingWriteBB(g_write_queue, req);
}
int nonblocking_write_sector(SectorDescriptor *sd, Voucher **v);


void blocking_read_sector(SectorDescriptor *sd, Voucher **v);
int nonblocking_read_sector(SectorDescriptor *sd, Voucher **v);


int redeem_voucher(Voucher *v, SectorDescriptor **sd);

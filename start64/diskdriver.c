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


struct voucher {
    pthread_mutex_t lock;
    pthread_cond_t done_cv;
    int done;
    int status;
    SectorDescriptor *sd;
};

typedef struct {
    SectorDescriptor *sd;
    Voucher *voucher;
    int is_read;
} DriverRequest;

static DiskDevice *g_dd;
static FreeSectorDescriptorStore *g_fsds;
static BoundedBuffer *g_write_queue;
static BoundedBuffer *g_read_queue;
static BoundedBuffer *g_free_requests;
static BoundedBuffer *g_free_vouchers;
static pthread_t g_writer_tid, g_reader_tid;


static DriverRequest *acquire_request_blocking(void)
{
    DriverRequest *req = (DriverRequest *)blockingReadBB(g_free_requests);
    req->sd = NULL;
    req->voucher = NULL;
    req->is_read = 0;
    return req;
}

static void release_request(DriverRequest *req)
{
    blockingWriteBB(g_free_requests, req);
}

static void release_voucher(Voucher *v)
{
    blockingWriteBB(g_free_vouchers, v);
}


static void *writer_thread_func(void *arg)
{
    (void)arg;

    for (;;) {
        DriverRequest *req = (DriverRequest *)blockingReadBB(g_write_queue);
        int ok = write_sector(g_dd, req->sd);

        /* write ownership returns to fsds after disk write attempt */
        blocking_put_sd(g_fsds, req->sd);

        /* for writes, redeem only needs success/failure */
        voucher_complete(req->voucher, ok, NULL);

        release_request(req);
    }

    return NULL;
}

static void *reader_thread_func(void *arg) {
  (void)arg;

    for (;;) {
        DriverRequest *req = (DriverRequest *)blockingReadBB(g_read_queue);
        int ok = read_sector(g_dd, req->sd);

        voucher_complete(req->voucher, ok, req->sd);

        release_request(req);
    }

    return NULL;
}

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
int nonblocking_write_sector(SectorDescriptor *sd, Voucher **v){
    DriverRequest *req;
    Voucher *new_v;

    if (!acquire_request_nonblocking(&req)) {
        *v = NULL;
        return 0;
    }

    if (!acquire_voucher_nonblocking(&new_v)) {
        release_request(req);
        *v = NULL;
        return 0;
    }

    req->sd = sd;
    req->voucher = new_v;
    req->is_read = 0;

    if (!nonblockingWriteBB(g_write_queue, req)) {
        release_request(req);
        release_voucher(new_v);
        *v = NULL;
        return 0;
    }

    *v = new_v;
    return 1;
}


void blocking_read_sector(SectorDescriptor *sd, Voucher **v){
    DriverRequest *req = acquire_request_blocking();
    Voucher *new_v = acquire_voucher_blocking();

    req->sd = sd;
    req->voucher = new_v;
    req->is_read = 1;

    *v = new_v;
    blockingWriteBB(g_read_queue, req);

}
int nonblocking_read_sector(SectorDescriptor *sd, Voucher **v){
    DriverRequest *req;
    Voucher *new_v;

    if (!acquire_request_nonblocking(&req)) {
        *v = NULL;
        return 0;
    }

    if (!acquire_voucher_nonblocking(&new_v)) {
        release_request(req);
        *v = NULL;
        return 0;
    }

    req->sd = sd;
    req->voucher = new_v;
    req->is_read = 1;

    if (!nonblockingWriteBB(g_read_queue, req)) {
        release_request(req);
        release_voucher(new_v);
        *v = NULL;
        return 0;
    }

    *v = new_v;
    return 1;
}


int redeem_voucher(Voucher *v, SectorDescriptor **sd){
    int status;
    pthread_mutex_lock(&v->lock);
    while (!v->done) {
        pthread_cond_wait(&v->done_cv, &v->lock);
    }

    status = v->status;
    *sd = v->sd;
    pthread_mutex_unlock(&v->lock);

    release_voucher(v);
    return status;
}

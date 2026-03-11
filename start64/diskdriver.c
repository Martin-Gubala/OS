/* 2888769g
COMPSCI4011 – Assessed Exercise
This is my own work*/

#include <pthread.h>
#include "diskdriver.h"
#include "BoundedBuffer.h"
#include "freesectordescriptorstore_full.h"
#include "sectordescriptorcreator.h"

#define DRIVER_QUEUE_CAPACITY 16
#define DRIVER_REQ_POOL_SIZE 64
#define DRIVER_VOUCHER_POOL_SIZE 64

struct voucher
{
    pthread_mutex_t lock;
    pthread_cond_t done_cv;
    int done;
    int status;
    SectorDescriptor *sd;
};

typedef struct
{
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
static pthread_t g_writer_tid;
static pthread_t g_reader_tid;

static DriverRequest g_request_nodes[DRIVER_REQ_POOL_SIZE];
static Voucher g_vouchers[DRIVER_VOUCHER_POOL_SIZE];

static DriverRequest *request_blocking(void)
{
    DriverRequest *req = (DriverRequest *)blockingReadBB(g_free_requests);
    req->sd = NULL;
    req->voucher = NULL;
    req->is_read = 0;
    return req;
}

static int request_nonblocking(DriverRequest **req_out)
{
    void *tmp = NULL;

    if (!nonblockingReadBB(g_free_requests, &tmp))
    {
        *req_out = NULL;
        return 0;
    }

    *req_out = (DriverRequest *)tmp;
    (*req_out)->sd = NULL;
    (*req_out)->voucher = NULL;
    (*req_out)->is_read = 0;
    return 1;
}

static Voucher *voucher_blocking(void)
{
    Voucher *v = (Voucher *)blockingReadBB(g_free_vouchers);
    v->done = 0;
    v->status = 0;
    v->sd = NULL;
    return v;
}

static int voucher_nonblocking(Voucher **v_out)
{
    void *tmp = NULL;

    if (!nonblockingReadBB(g_free_vouchers, &tmp))
    {
        *v_out = NULL;
        return 0;
    }

    *v_out = (Voucher *)tmp;
    (*v_out)->done = 0;
    (*v_out)->status = 0;
    (*v_out)->sd = NULL;
    return 1;
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

    for (;;)
    {
        DriverRequest *req = (DriverRequest *)blockingReadBB(g_write_queue);
        int status = write_sector(g_dd, req->sd);

        blocking_put_sd(g_fsds, req->sd);

        pthread_mutex_lock(&req->voucher->lock);
        req->voucher->status = status;
        req->voucher->sd = NULL;
        req->voucher->done = 1;
        pthread_cond_signal(&req->voucher->done_cv);
        pthread_mutex_unlock(&req->voucher->lock);

        release_request(req);
    }

    return NULL;
}

static void *reader_thread_func(void *arg)
{
    (void)arg;

    for (;;)
    {
        DriverRequest *req = (DriverRequest *)blockingReadBB(g_read_queue);
        int status = read_sector(g_dd, req->sd);

        pthread_mutex_lock(&req->voucher->lock);
        req->voucher->status = status;
        req->voucher->sd = req->sd;
        req->voucher->done = 1;
        pthread_cond_signal(&req->voucher->done_cv);
        pthread_mutex_unlock(&req->voucher->lock);

        release_request(req);
    }

    return NULL;
}

void init_disk_driver(DiskDevice *dd, void *mem_start, unsigned long mem_length, FreeSectorDescriptorStore **fsds)
{
    int i;
    FreeSectorDescriptorStore *store;
    g_dd = dd;
    store = create_fsds();
    create_free_sector_descriptors(store, mem_start, mem_length);
    g_fsds = store;
    *fsds = store;

    g_write_queue = createBB(DRIVER_QUEUE_CAPACITY);
    g_read_queue = createBB(DRIVER_QUEUE_CAPACITY);

    g_free_requests = createBB(DRIVER_REQ_POOL_SIZE);
    g_free_vouchers = createBB(DRIVER_VOUCHER_POOL_SIZE);

    for (i = 0; i < DRIVER_REQ_POOL_SIZE; i++)
    {
        g_request_nodes[i].sd = NULL;
        g_request_nodes[i].voucher = NULL;
        g_request_nodes[i].is_read = 0;
        blockingWriteBB(g_free_requests, &g_request_nodes[i]);
    }

    for (i = 0; i < DRIVER_VOUCHER_POOL_SIZE; i++)
    {
        pthread_mutex_init(&g_vouchers[i].lock, NULL);
        pthread_cond_init(&g_vouchers[i].done_cv, NULL);
        g_vouchers[i].done = 0;
        g_vouchers[i].status = 0;
        g_vouchers[i].sd = NULL;
        blockingWriteBB(g_free_vouchers, &g_vouchers[i]);
    }
    pthread_create(&g_writer_tid, NULL, writer_thread_func, NULL);
    pthread_create(&g_reader_tid, NULL, reader_thread_func, NULL);
}
void blocking_write_sector(SectorDescriptor *sd, Voucher **v)
{
    DriverRequest *req = request_blocking();
    Voucher *new_v = voucher_blocking();

    req->sd = sd;
    req->voucher = new_v;
    req->is_read = 0;

    *v = new_v;
    blockingWriteBB(g_write_queue, req);
}
int nonblocking_write_sector(SectorDescriptor *sd, Voucher **v)
{
    DriverRequest *req = NULL;
    Voucher *new_v;

    if (!request_nonblocking(&req))
    {
        *v = NULL;
        return 0;
    }

    if (!voucher_nonblocking(&new_v))
    {
        release_request(req);
        *v = NULL;
        return 0;
    }

    req->sd = sd;
    req->voucher = new_v;
    req->is_read = 0;

    if (!nonblockingWriteBB(g_write_queue, req))
    {
        release_request(req);
        release_voucher(new_v);
        *v = NULL;
        return 0;
    }

    *v = new_v;
    return 1;
}

void blocking_read_sector(SectorDescriptor *sd, Voucher **v)
{
    DriverRequest *req = request_blocking();
    Voucher *new_v = voucher_blocking();

    req->sd = sd;
    req->voucher = new_v;
    req->is_read = 1;

    *v = new_v;
    blockingWriteBB(g_read_queue, req);
}
int nonblocking_read_sector(SectorDescriptor *sd, Voucher **v)
{
    DriverRequest *req;
    Voucher *new_v;

    if (!request_nonblocking(&req))
    {
        *v = NULL;
        return 0;
    }

    if (!voucher_nonblocking(&new_v))
    {
        release_request(req);
        *v = NULL;
        return 0;
    }

    req->sd = sd;
    req->voucher = new_v;
    req->is_read = 1;

    if (!nonblockingWriteBB(g_read_queue, req))
    {
        release_request(req);
        release_voucher(new_v);
        *v = NULL;
        return 0;
    }

    *v = new_v;
    return 1;
}

int redeem_voucher(Voucher *v, SectorDescriptor **sd)
{
    int status;
    pthread_mutex_lock(&v->lock);
    while (!v->done)
    {
        pthread_cond_wait(&v->done_cv, &v->lock);
    }

    status = v->status;
    *sd = v->sd;
    pthread_mutex_unlock(&v->lock);

    release_voucher(v);
    return status;
}

/**
 * This file is for implementation of MIMPI library.
 * */

#include "stdint.h"
#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>

static int world_size = 0;
static int world_rank = -1;

int channels[16][16][2]; // not used fields are marked with -1

// IN BROADCAST WE USE FROM 0 INDEXING

int tree[16][4];
int edge_n = 0;

// tree[i][0] READ_FROM_UP
// tree[i][1] WRITE_TO_DOWN
// tree[i][0] READ_FROM_DOWN
// tree[i][1] WRITE_TO_UP


#define READ_FROM_UP   0
#define WRITE_TO_DOWN  1
#define READ_FROM_DOWN 2
#define WRITE_TO_UP    3

// tree is indexed from 0

int virtual_tree[16] = {0, 1, 2, 3, 4, 5,6,7,
                        8,9,10,11,12,13,14,15};

// function assume that from and to are correct
// it return -1 if channel is closed
int get_send_fd(const int to) {
    return channels[world_rank][to][1];
}

// function assume that from and to are correct
// it return -1 if channel is closed
int get_recv_fd(const int from) {
    return channels[from][world_rank][0];
}


atomic_bool running = false;

typedef struct packet {
    int tag;
    int source;
    int data_length;
    int struct_size;
    char data[];
} packet_t;

typedef struct {
    packet_t **data;
    size_t size;
    size_t capacity;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
} SharedBuffer;

void initBuffer(SharedBuffer *buffer, size_t initialCapacity) {
    buffer->data = (packet_t **)malloc(initialCapacity * sizeof(packet_t *));
    buffer->size = 0;
    buffer->capacity = initialCapacity;

    pthread_mutex_init(&buffer->mutex, NULL);
    pthread_cond_init(&buffer->cond, NULL);
}

void resizeBuffer(SharedBuffer *buffer) {
    buffer->capacity *= 2;
    buffer->data = (packet_t **)realloc(buffer->data, buffer->capacity * sizeof(packet_t *));
}

void addToBuffer(SharedBuffer *buffer, packet_t *packet) {
    pthread_mutex_lock(&buffer->mutex);

    if (buffer->size == buffer->capacity) {
        resizeBuffer(buffer);
    }

    buffer->data[buffer->size++] = packet;

    // Signal that data is available
    pthread_cond_signal(&buffer->cond);
    pthread_mutex_unlock(&buffer->mutex);
}

packet_t *removeFromBuffer(SharedBuffer *buffer) {
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->size == 0) {
        // Wait for data to be available
        pthread_cond_wait(&buffer->cond, &buffer->mutex);
    }

    packet_t *packet = buffer->data[--buffer->size];

    pthread_mutex_unlock(&buffer->mutex);

    return packet;
}

packet_t *removeFromBufferByValues(SharedBuffer *buffer, int tag, int source, int data_length) {
    pthread_mutex_lock(&buffer->mutex);

    packet_t *removedPacket = NULL;

    for (size_t i = 0; i < buffer->size; ++i) {
        if (buffer->data[i]->tag == tag && buffer->data[i]->source == source && buffer->data[i]->data_length == data_length) {
            // Found the matching packet, remove it
            removedPacket = buffer->data[i];

            // Shift remaining packets to eliminate blank spots
            for (size_t j = i; j < buffer->size - 1; ++j) {
                buffer->data[j] = buffer->data[j + 1];
            }

            --buffer->size;
            break;
        }
    }

    pthread_mutex_unlock(&buffer->mutex);

    return removedPacket;
}

void destroyBuffer(SharedBuffer *buffer) {
    free(buffer->data);
    pthread_mutex_destroy(&buffer->mutex);
    pthread_cond_destroy(&buffer->cond);
}

SharedBuffer* p_buff;

//pthread_cond_t packet_arrive;
//pthread_mutex_t mut;

typedef struct args {
    int id;
    SharedBuffer* p_buffer;
    pthread_cond_t* p_packet_arrive;
} args_t;

// musimy wiedziec na co teraz czeka P
// ale kazdy worker czyta tyle ile moze


typedef struct packet_header {
    int tag;
    int data_length;
} packet_header_t;

// Memory allocation and initialisation of structure
packet_t * create_packet(int tag, int source, int length_left)
{
    // Allocating memory according to user provided
    // array of characters
    packet_t* p = malloc(sizeof(packet_t) + sizeof(char) * length_left);

    p->tag = tag;
    p->data_length = length_left;
    p->source = source;

    // Assigning size according to size of stud_name
    // which is a copy of user provided array a[].
    p->struct_size = (sizeof(*p) + sizeof(char) * length_left);

    return p;
}

void print_packet(packet_t* p) {
    printf("tag: %d\n", p->tag);
    printf("data_length: %d\n", p->data_length);
    printf("source: %d\n", p->source);
    printf("struct_size: %d\n", p->struct_size);
}

// don't end until readed n bytes
/// read exacly n bytes, return false if piped closed \n
/// caller must guranty that buf is okey
bool read_n(const int fd, void* buf,const int n) {
    int rec = 0;
    int left = n;
    int received = 0;

    while (left > 0) {
        received += rec;
        rec = chrecv(fd, buf + received, n);
        if (rec == 0) {
            return false;
        }
        left -= rec;
    }

    return true;
}

void* worker(void* data)
{
    args_t args = *((args_t *)data);

    SharedBuffer* p_buffer = args.p_buffer;
    pthread_cond_t* p_packet_arrive = args.p_packet_arrive;
    int id = args.id;

    int rec_ds = channels[id][world_rank][0];
    char tmp_buff[PIPE_BUF];
    int rec = 0;
    packet_t  packet;
    packet_header_t header;


    while (running) {
//        printf("SIEMA from %d\n", args.id);
        // musi wiedziec od kogo czyta
        // wordk rank + id

        if (read_n(rec_ds, &header, sizeof (packet_header_t))) {
            assert(header.data_length > 0);

            packet_t* p_packet = create_packet(header.tag, id, header.data_length);

            if (read_n(rec_ds, &p_packet->data, header.data_length)) {
                print_packet(p_packet);

                addToBuffer(p_buffer, p_packet);
            }


        } else {
            // TODO co zrobic jak pipe byl closed

        }
    }

    free(data);

    return 0;
}




void MIMPI_Init(bool enable_deadlock_detection) {
    channels_init();

    const char* num_proc_str = getenv("MIMPI_NUM_PROCESSES");
    const char* rank_str = getenv("MIMPI_RANK");

    if (num_proc_str == NULL) {
        fprintf(stderr, "Error: MIMPI_NUM_PROCESSES not set or invalid\n");
        exit(EXIT_FAILURE);
    }
    world_size = atoi(num_proc_str);
    if (world_size <= 0) {
        fprintf(stderr, "Error: MIMPI_NUM_PROCESSES not set or invalid\n");
        exit(EXIT_FAILURE);
    }


    if (rank_str == NULL) {
        fprintf(stderr, "Error: MIMPI_RANK not set\n");
        exit(EXIT_FAILURE);
    }
    world_rank = atoi(rank_str);
    if (world_rank < 0 || world_rank >= world_size) {
        fprintf(stderr, "Error: Invalid MIMPI_RANK\n");
        exit(EXIT_FAILURE);
    }


    const char* channels_fd_str = getenv("MIMPI_CHANNELS_FD");
    if (channels_fd_str == NULL) {
        fprintf(stderr, "Error: MIMPI_CHANNELS_FD pointer is not set or invalid\n");
        exit(EXIT_FAILURE);
    }
    const int channel_fd = atoi(channels_fd_str);
    if (channel_fd <= 0) {
        fprintf(stderr, "Error: MIMPI_CHANNELS_FD number is below or equal to 0\n");
        exit(EXIT_FAILURE);
    }

    if (debug)
        printf("CHANNEL_FD: %d\n", channel_fd);


    for (int i = 0; i < 16; ++i) {
        for (int j = 0; j < 16; ++j) {
            for (int k = 0; k < 2; ++k) {
                channels[i][j][k] = -1;
            }
        }
    }

    int n = world_size;
    int tmp_fd = channel_fd;
    for (int i = 0; i < n; ++i) {
        for (int j = i + 1; j < n; ++j) {
            channels[i][j][0] = tmp_fd++;
            channels[i][j][1] = tmp_fd++;
            channels[j][i][0] = tmp_fd++;
            channels[j][i][1] = tmp_fd++;
        }
    }

    if (n == 1) {
        ASSERT_SYS_OK(close(tmp_fd++));
        ASSERT_SYS_OK(close(tmp_fd++));
    }

    edge_n = n - 1;

    for (int i = 0; i < edge_n; ++i) {
        tree[i][READ_FROM_UP] = -1;
        tree[i][READ_FROM_DOWN] = -1;
        tree[i][WRITE_TO_DOWN] = -1;
        tree[i][WRITE_TO_UP] = -1;
    }

    for (int i = 0; i < edge_n; ++i) {
        tree[i][READ_FROM_UP] = tmp_fd++;
        tree[i][WRITE_TO_DOWN] = tmp_fd++;
        tree[i][READ_FROM_DOWN] = tmp_fd++;
        tree[i][WRITE_TO_UP] = tmp_fd++;
    }

    // closing not used channels

    for (int i = 0; i < n; ++i) {
        for (int j = 0; j < n; ++j) {
            if (i == j)
                continue;

            if (i == world_rank || j == world_rank) {
                if (i == world_rank) {
                    // we are closing reading end
                    ASSERT_SYS_OK(close(channels[i][j][0]));
                }
                if (j == world_rank) {
                    // closing writing end
                    ASSERT_SYS_OK(close(channels[i][j][1]));
                }
            } else {
                ASSERT_SYS_OK(close(channels[i][j][0]));
                ASSERT_SYS_OK(close(channels[i][j][1]));
            }
        }
    }

    int id = world_rank;
    // edges numbers if exists
    int from_father = id - 1;
    int l_child = id * 2 + 1 < world_size ? id * 2  : -1;
    int r_child = id * 2 + 2 < world_size ? id * 2 + 1 : -1;


    for (int i = 0; i < edge_n; ++i) {
        if (i == from_father) {
            // close father read and write
            ASSERT_SYS_OK(close(tree[from_father][READ_FROM_DOWN]));
            ASSERT_SYS_OK(close(tree[from_father][WRITE_TO_DOWN]));
        } else if (i == l_child) {
            ASSERT_SYS_OK(close(tree[l_child][READ_FROM_UP]));
            ASSERT_SYS_OK(close(tree[l_child][WRITE_TO_UP]));
        } else if (i == r_child) {
            ASSERT_SYS_OK(close(tree[r_child][READ_FROM_UP]));
            ASSERT_SYS_OK(close(tree[r_child][WRITE_TO_UP]));
        } else {
            ASSERT_SYS_OK(close(tree[i][READ_FROM_DOWN]));
            ASSERT_SYS_OK(close(tree[i][WRITE_TO_DOWN]));
            ASSERT_SYS_OK(close(tree[i][READ_FROM_UP]));
            ASSERT_SYS_OK(close(tree[i][WRITE_TO_UP]));
        }
    }


    // Create thread attributes.
    pthread_attr_t attr;
    ASSERT_ZERO(pthread_attr_init(&attr));
    ASSERT_ZERO(pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED ));

    running = true;

    initBuffer(p_buff, 10);

    ASSERT_ZERO(pthread_mutex_init(&p_buff->mutex, NULL));
    ASSERT_ZERO(pthread_cond_init(&p_buff->cond, NULL));

    // Create (and start) the threads.
    pthread_t threads[n];
    for (int i = 0; i < n; i++) {
        if (i == world_rank) {
            args_t * worker_arg = malloc(sizeof(args_t));
            worker_arg->id = i;
            worker_arg->p_buffer = p_buff;
            worker_arg->p_packet_arrive = &p_buff->cond;
            ASSERT_ZERO(pthread_create(&threads[i], &attr, worker, worker_arg));
        }
    }
}

void MIMPI_Finalize() {
    // pozamykac wszystkie channele ktorych bysme potrzebowali

    running = false;

    for (int i = 0; i < world_size; ++i) {
        for (int j = 0; j < world_size; ++j) {
            if (i == j)
                continue;

            if (i == world_rank || j == world_rank) {
                if (i == world_rank) {
                    // we are closing reading end
                    ASSERT_SYS_OK(close(channels[i][j][1]));
                }
                if (j == world_rank) {
                    // closing writing end
                    ASSERT_SYS_OK(close(channels[i][j][0]));
                }
            }
        }
    }

    int id = world_rank;
    // edges numbers if exists
    int from_father = id - 1;
    int l_child = id * 2 + 1 < world_size ? id * 2 : -1;
    int r_child = id * 2 + 2 < world_size ? id * 2 + 1 : -1;


    for (int i = 0; i < edge_n; ++i) {
        if (i == from_father) {
            // close father read and write
            ASSERT_SYS_OK(close(tree[from_father][READ_FROM_UP]));
            ASSERT_SYS_OK(close(tree[from_father][WRITE_TO_UP]));
        } else if (i == l_child) {
            ASSERT_SYS_OK(close(tree[l_child][READ_FROM_DOWN]));
            ASSERT_SYS_OK(close(tree[l_child][WRITE_TO_DOWN]));
        } else if (i == r_child) {
            ASSERT_SYS_OK(close(tree[r_child][READ_FROM_DOWN]));
            ASSERT_SYS_OK(close(tree[r_child][WRITE_TO_DOWN]));
        }
    }

    if (debug) print_open_descriptors();

//    print_open_descriptors();

    ASSERT_ZERO(pthread_cond_destroy(&p_buff->cond));
    ASSERT_ZERO(pthread_mutex_destroy(&p_buff->mutex));

    channels_finalize();
}

int MIMPI_World_size() {
    return world_size;
}

int MIMPI_World_rank() {
    return world_rank;
}

MIMPI_Retcode MIMPI_Send(
    void const *data,
    int count,
    int destination,
    int tag
) {
    if (debug) printf("send\n");

    if (world_rank == destination)
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;

    if (destination >= world_size || destination < 0)
        return MIMPI_ERROR_NO_SUCH_RANK;

    int send_fd = get_send_fd(destination);

    if (chsend(send_fd, data, count) == -1 && errno == EPIPE) {
        if (debug) printf("finished send rank: %d\n", world_rank);
        return MIMPI_ERROR_REMOTE_FINISHED;
    }

    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Recv(
    void *data,
    int count,
    int source,
    int tag
) {
    if (debug) printf("rec\n");


    if (world_rank == source ) {
        if (debug) {
            printf("finished recv error SELF rank: %d\n", world_rank);
        }
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    }

    if (source >= world_size || source < 0) {
        if (debug) {
            printf("finished recv error RANK rank: %d\n", world_rank);
        }

        return MIMPI_ERROR_NO_SUCH_RANK;
    }

    int recv_fd = get_recv_fd(source);

    int left = count;
    int rec = 0;
    int received = 0;

    while (left > 0) {
        received += rec;

        rec = chrecv(recv_fd, data + received, left);

        if (rec == 0) {
            if (debug) printf("finished recv rank: %d\n", world_rank);
            return MIMPI_ERROR_REMOTE_FINISHED;
        }

        left -= rec;
    }

    if (debug) printf("get recv %d wordl rank: %d\n", get_recv_fd(source), world_rank);

    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Barrier() {
    // jesli mamy dzieci czekamy na nie
    // kiedy dzieci skoncza wysylamy sygnal do ojca i czekamy na ojca

    if (world_size == 1) return MIMPI_SUCCESS;

    int id = world_rank;
    char dummy = 0;
    int rec = 0;
    int l_child = id * 2;
    int r_child = id * 2 + 1;
    int father = (id - 1);


    if (2 * id + 1 < world_size) {
        rec = chrecv(tree[l_child][READ_FROM_DOWN], &dummy, 1);

        if (rec == 0) {
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }

    if (2 * id + 2 < world_size) {
        rec = chrecv(tree[r_child][READ_FROM_DOWN], &dummy, 1);

        if (rec == 0) {
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }

    // send that we are ready
    if (id != 0) {
        if (chsend(tree[father][WRITE_TO_UP], &dummy, 1) == -1 ) {
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }

    // czekamy az obudzi nas ojciec
    if (id != 0) {
        rec = chrecv(tree[father][READ_FROM_UP], &dummy, 1);

        if (rec == 0) {
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }

    // budzimy dzieci
    if (2 * id + 1 < world_size) {
        if (chsend(tree[l_child][WRITE_TO_DOWN], &dummy, 1) == -1) {
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }

    if (2 * id + 2 < world_size) {
        if (chsend(tree[r_child][WRITE_TO_DOWN], &dummy, 1) == -1) {
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }

    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Bcast(
    void *data,
    int count,
    int root
) {
    if (root >= world_size || root < 0) {
        if (debug) {
            printf("BCAST error RANK rank: %d\n", world_rank);
        }

        return MIMPI_ERROR_NO_SUCH_RANK;
    }

    MIMPI_Barrier();

    virtual_tree[root] = 0;
    virtual_tree[0] = root;

    int i = virtual_tree[world_rank];

    if (i == 0) {
        int l = virtual_tree[i * 2 + 1];
        int r = virtual_tree[i * 2 + 2];
        if (i * 2 + 1 < world_size) MIMPI_Send(data, count, l, MIMPI_ANY_TAG);
        if (i * 2 + 2 < world_size) MIMPI_Send(data, count, r, MIMPI_ANY_TAG);
    }

    if (i != 0) {
        int father = virtual_tree[(i - 1) / 2];
        int l = virtual_tree[i * 2 + 1];
        int r = virtual_tree[i * 2 + 2];

        MIMPI_Recv(data, count, father, MIMPI_ANY_TAG);
        if (i * 2 + 1 < world_size) MIMPI_Send(data, count, l, MIMPI_ANY_TAG);
        if (i * 2 + 2 < world_size) MIMPI_Send(data, count, r, MIMPI_ANY_TAG);
    }



    return MIMPI_SUCCESS;
}

void calculate(uint8_t* buff, const uint8_t * recv_data,int count, MIMPI_Op op) {
    for (int i = 0; i < count; ++i) {
        if (op == MIMPI_MAX) {
            buff[i] = MAX(buff[i], recv_data[i]);
        } else if (op == MIMPI_MIN) {
            buff[i] = MIN(buff[i], recv_data[i]);
        } else if (op == MIMPI_SUM) {
            buff[i] = buff[i] + recv_data[i];
        } else if (op == MIMPI_PROD) {
            buff[i] = buff[i] * recv_data[i];
        }
    }
}

MIMPI_Retcode MIMPI_Reduce(
    void const *send_data,
    void *recv_data,
    int count,
    MIMPI_Op op,
    int root
) {
    if (root >= world_size || root < 0) {
        if (debug) {
            printf("REDUCE error RANK rank: %d\n", world_rank);
        }

        return MIMPI_ERROR_NO_SUCH_RANK;
    }

    MIMPI_Barrier();

    uint8_t * buff = malloc(count);
    uint8_t * rec_buff = malloc(count);
    memcpy(buff, send_data, count);

    virtual_tree[root] = 0;
    virtual_tree[0] = root;
    int i = virtual_tree[world_rank];

    // czekamy az dostaniemy czesciowe wyniki od dzieci

    if (i * 2 + 1 < world_size) {
        int l = virtual_tree[i * 2 + 1];
        MIMPI_Recv(rec_buff, count, l, MIMPI_ANY_TAG);
        // dostalismy
        calculate(buff, rec_buff, count, op);
    }

    if (i * 2 + 2 < world_size) {
        int r = virtual_tree[i * 2 + 2];
        MIMPI_Recv(rec_buff, count, r, MIMPI_ANY_TAG);
        // dostalismy
        calculate(buff, rec_buff, count, op);
    }

    // wysylamy do ojca swoje czesciowe wyniki

    if (i != 0) {
        int father = virtual_tree[(i - 1) / 2];
        MIMPI_Send(buff, count, father, MIMPI_ANY_TAG);
    } else {
        // jestesmy rootem czyli mamy juz u siebie ostateczny wynik
        memcpy(recv_data, buff, count);
    }

    free(buff);
    free(rec_buff);

    return MIMPI_SUCCESS;
}
/**
 * This file is for implementation of MIMPI library.
 * */

#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"

static int world_size = 0;
static int world_rank = -1;

int channels[16][16][2]; // not used fields are marked with -1

// IN BROADCAST WE USE FROM 1 INDEXING
int broadcast_tree[17][2];




int* get_curr(int id) {
    return broadcast_tree[id];
}

int* get_father(int id) {
    return broadcast_tree[id/2];
}

int* get_left_child(int id) {
    if (id * 2 > world_size) return NULL;

    return broadcast_tree[id * 2];
}

int* get_right_child(int id) {
    if (id * 2 + 1> world_size) return NULL;

    return broadcast_tree[id * 2 + 1];
}

void close_read(const int* id) {
    if (id == NULL) return;

//    if (world_rank == 0) printf("closing read: %d RANK %d\n", id[0], world_rank + 1);

    if (id[0] != -1) ASSERT_SYS_OK(close(id[0]));
}

void close_write(const int* id) {
    if (id == NULL) return;

//    if (world_rank == 0) printf("closing write : %d RANK %d\n", id[1], world_rank + 1);

    if (id[1] != -1) ASSERT_SYS_OK(close(id[1]));
}

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

    for (int i = 0; i <= 16; ++i) {
        for (int j = 0; j < 2; ++j) {
            broadcast_tree[i][j] = -1;
        }
    }

    for (int i = 1; i <= n; ++i) {
        broadcast_tree[i][0] = tmp_fd++;
        broadcast_tree[i][1] = tmp_fd++;
    }

    // closing not used channels

    for (int i = 0; i < n; ++i) {
        for (int j = 0; j < n; ++j) {
            if (i == j)
                continue;

            if (i == world_rank || j == world_rank) {
                if (i == world_rank) {
                    // we are closing reading end
//                    printf("closing fd: %d\n", channels[i][j][0]);
                    ASSERT_SYS_OK(close(channels[i][j][0]));
                }
                if (j == world_rank) {
                    // closing writing end
//                    printf("closing fd: %d\n", channels[i][j][1]);
                    ASSERT_SYS_OK(close(channels[i][j][1]));
                }
            } else {
//                printf("closing fd: %d\n", channels[i][j][0]);
                ASSERT_SYS_OK(close(channels[i][j][0]));
//                printf("closing fd: %d\n", channels[i][j][1]);
                ASSERT_SYS_OK(close(channels[i][j][1]));
            }
        }
    }

////     closing broadcast_tree channels
//    for (int i = 0; i < n; ++i) {
//        if (i == world_rank) {
//            ASSERT_SYS_OK(close(broadcast_tree[i][1]));
//        } else {
//            ASSERT_SYS_OK(close(broadcast_tree[i][0]));
//        }
//    }

    int id = world_rank + 1;
    int* us = get_curr(id);
    int* father = get_father(id);
    int* l_child = get_left_child(id);
    int* r_child = get_right_child(id);

    for (int i = 1; i <= n; ++i) {
        int* curr= get_curr(i);

        if (curr == us) {
            close_write(us);
        } else if (curr == father) {
            close_read(father);
        } else if (curr == l_child) {
            close_read(l_child);
        } else if (curr == r_child) {
            close_read(r_child);
        } else {
            ASSERT_SYS_OK(close(curr[0]));
            ASSERT_SYS_OK(close(curr[1]));
        }
    }

//    if (debug) {
//        if (world_rank == 0 ) {
//            for (int i = 0; i < n; ++i) {
//                for (int j = 0; j < n; ++j) {
//                    printf(" (%d, %d) ", channels[i][j][0], channels[i][j][1]);
//                }
//                printf("\n");
//            }
//        }
//    }

//    if (debug) {
//        if (world_rank == 0) {
//            for (int i = 1; i <= n; ++i) {
//                printf(" (%d, %d) ", broadcast_tree[i][0], broadcast_tree[i][1]);
//            }
//            printf("\n");
//        }
//    }

}

void MIMPI_Finalize() {
    // pozamykac wszystkie channele ktorych bysme potrzebowali

    for (int i = 0; i < world_size; ++i) {
        for (int j = 0; j < world_size; ++j) {
            if (i == j)
                continue;

            if (i == world_rank || j == world_rank) {
                if (i == world_rank) {
                    // we are closing reading end
//                    printf("closing fd: %d\n", channels[i][j][1]);
                    ASSERT_SYS_OK(close(channels[i][j][1]));
                }
                if (j == world_rank) {
                    // closing writing end
//                    printf("closing fd: %d\n", channels[i][j][0]);
                    ASSERT_SYS_OK(close(channels[i][j][0]));
                }
            }
        }
    }

    int id = world_rank + 1;
    int* us = get_curr(id);
    int* father = get_father(id);
    int* l_child = get_left_child(id);
    int* r_child = get_right_child(id);

    for (int i = 1; i <= world_size; ++i) {
        int* curr= get_curr(i);

        if (curr == us) {
            close_read(us);
        } else if (curr == father) {
            close_write(father);
        } else if (curr == l_child) {
            close_write(l_child);
        } else if (curr == r_child) {
            close_write(r_child);
        }
    }

    if (debug) print_open_descriptors();

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

    if (chsend(send_fd, data, count) == -1) {
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

//    if (chrecv(recv_fd, data, count) == 0) {
//        if (debug) printf("finished recv rank: %d\n", world_rank);
//
//        return MIMPI_ERROR_REMOTE_FINISHED;
//    }

    int left = count;
    int rec = 0;

    while (left > 0) {
        rec = chrecv(recv_fd, data, left);

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

    int id = world_rank + 1;
    int* us = get_curr(id);
    char dummy = 0;

    if (id * 2 <= world_size) {
//        printf("R: %d, czeka na LD na ds: %d\n", id, us[0]);
        ASSERT_SYS_OK(chrecv(us[0], &dummy, 1));
    }

    if (id * 2 + 1 <= world_size) {
//        printf("R: %d, czeka na PD na ds: %d\n", id, us[0]);
        ASSERT_SYS_OK(chrecv(us[0], &dummy, 1));
    }

    int* father = get_father(id);
    // send that we are ready
    if (id != 1) {
//        printf("R: %d wysyla do ojca na ds %d\n", id, father[1]);
        chsend(father[1], &dummy, 1);
    }

    // czekamy az nas obudzi
    if (id != 1) {
//        printf("R: %d czeka na obudzenie na ds %d\n", id, us[0]);
        chrecv(us[0], &dummy, 1);
    }

    // budzimy dzieci
    if (id * 2 <= world_size) {
        int* l_child = get_left_child(id);

        if (l_child != NULL) {
//            printf("R: %d budzi L ds: %d\n", id, l_child[1]);
            ASSERT_SYS_OK(chsend(l_child[1], &dummy, 1));
        }

    }

    if (id * 2 + 1 <= world_size) {
        int* r_child = get_right_child(id);

        if (r_child != NULL) {
//            printf("R: %d budzi R ds: %d\n", id, r_child[1]);
            ASSERT_SYS_OK(chsend(r_child[1], &dummy, 1));
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

//    if (world_rank == root) {
//        for (int i = 0; i < world_size; ++i) {
//            if (i != world_rank) {
//                MIMPI_Send(data, count, i, MIMPI_ANY_TAG);
//            }
//        }
//    }
//
//    if (world_rank != root)
//        MIMPI_Recv(data, count, root, MIMPI_ANY_TAG);

    int i = world_rank;

    if (world_rank == root) {
        if (i * 2 + 1 < world_size) MIMPI_Send(data, count, i * 2 + 1, MIMPI_ANY_TAG);
        if (i * 2 + 2 < world_size) MIMPI_Send(data, count, i * 2 + 2, MIMPI_ANY_TAG);
    }

    if (world_rank != root) {
        int father = (i - 1) / 2;

        MIMPI_Recv(data, count, father, MIMPI_ANY_TAG);
        if (i * 2 + 1 < world_size) MIMPI_Send(data, count, i * 2 + 1, MIMPI_ANY_TAG);
        if (i * 2 + 2 < world_size) MIMPI_Send(data, count, i * 2 + 2, MIMPI_ANY_TAG);
    }



    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Reduce(
    void const *send_data,
    void *recv_data,
    int count,
    MIMPI_Op op,
    int root
) {
    TODO
}
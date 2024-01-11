/**
 * This file is for implementation of MIMPI library.
 * */

#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"

static int world_size = 0;
static int world_rank = -1;

int channels[16][16][2]; // not used fields are marked with -1

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

    if (debug) {
        if (world_rank == 0 ) {
            for (int i = 0; i < n; ++i) {
                for (int j = 0; j < n; ++j) {
                    printf(" (%d, %d) ", channels[i][j][0], channels[i][j][1]);
                }
                printf("\n");
            }
        }
    }
}

void MIMPI_Finalize() {
    // pozamykac wszystkie channele ktorych bysme potrzebowali

//    for (int i = 0; i < world_size; ++i) {
//        if (channels[world_rank][i][0] != -1 && channels[i][world_rank][0] != -1) {
//            printf("closing: %d\n", channels[world_rank][i][0]);
//            printf("closing: %d\n", channels[world_rank][i][1]);
//            printf("closing: %d\n", channels[i][world_rank][0]);
//            printf("closing: %d\n", channels[i][world_rank][1]);
//
//            ASSERT_SYS_OK(close(channels[world_rank][i][0]));
//            ASSERT_SYS_OK(close(channels[world_rank][i][1]));
//            ASSERT_SYS_OK(close(channels[i][world_rank][0]));
//            ASSERT_SYS_OK(close(channels[i][world_rank][1]));
//
//        }
//    }

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

//    print_open_descriptors();

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

    if (chrecv(recv_fd, data, count) == 0) {
        if (debug) printf("finished recv rank: %d\n", world_rank);

        return MIMPI_ERROR_REMOTE_FINISHED;
    }

    if (debug) printf("get recv %d wordl rank: %d\n", get_recv_fd(source), world_rank);

    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Barrier() {
    TODO
}

MIMPI_Retcode MIMPI_Bcast(
    void *data,
    int count,
    int root
) {
    TODO
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
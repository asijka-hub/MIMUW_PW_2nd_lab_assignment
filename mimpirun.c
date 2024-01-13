/**
 * This file is for implementation of mimpirun program.
 * */

#include "mimpi_common.h"
#include "channel.h"

// set enviroment variable to indicate which is first fd that we used tor channels
// return first channel number

int sync_channel[2];

int create_channels(int n) {

    int fd[2];

    channel(fd);

    while (fd[0] <= 20 && fd[1] <= 20) {
        channel(fd);
    }

    char buff[6];
    sprintf(buff, "%d", fd[0]);
    int minimal = fd[0];
    setenv("MIMPI_CHANNELS_FD", buff, 1);

    if (debug) printf("minimal channel number is: %d\n", fd[0]);

    // stworzyc channele ile ich ma byc?
    // n * (n-1) / 2 * 2 = n * (n - 1)
    // TODO pomyslec co sie dzieje jak n=1

    int channel_n = n * (n - 1) - 1; // 1 zostal channel juz stworzony

    if (debug) printf("channel_n: %d\n", channel_n);

    for (int i = 0; i < channel_n; ++i) {
        channel(fd);
        //TODO assert ze sa w dobrej kolejnosci
    }

    // stworzyc n chaneli na broadcast
    for (int i = 0; i < n; ++i) {
        channel(fd);
    }

    // stworzy sync pipe
    channel(sync_channel);


    return minimal;
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <number_of_processes> <program> [args...]\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    const int n = atoi(argv[1]);
    const char *prog = argv[2];
    char **args = &argv[2];
    int rank = -1;
    char rank_buf[6];

    setenv("MIMPI_NUM_PROCESSES", argv[1], 1);

    channels_init();

    int minimal_fd = create_channels(n);

    char idx = 1;
    for (int i = 0; i < n; ++i) {
        chsend(sync_channel[1], &idx, 1);
        idx++;
    }

    for (int i = 0; i < n; i++) {
        rank++;
        int pid = fork();
        if (pid == 0) {  // Proces potomny
            sprintf(rank_buf, "%d", rank);
            setenv("MIMPI_RANK", rank_buf, 1);
            execvp(prog, args);
            exit(EXIT_FAILURE);
        } else if (pid < 0) {
            perror("Fork failed");
            exit(EXIT_FAILURE);
        }
    }

    int channel_n = n > 1 ? n * (n - 1) * 2 : 2;

    for (int i = minimal_fd; i < minimal_fd + channel_n; ++i) {
        ASSERT_SYS_OK(close(i));
    }

    int broadcast_fd = minimal_fd + channel_n;

    for (int i = broadcast_fd; i < broadcast_fd + n * 2; ++i) {
        ASSERT_SYS_OK(close(i));
    }

    int sync_fd = broadcast_fd + n * 2;

    for (int i = sync_fd; i < sync_fd + 2; ++i) {
        ASSERT_SYS_OK(close(i));
    }

    if (debug) print_open_descriptors();

    for (int i = 0; i < n; i++) {
        int status;
        ASSERT_SYS_OK(wait(&status));
    }

    channels_finalize();
    exit(EXIT_SUCCESS);
}
#include <stdio.h>
#include <string.h>
#include <zmq.h>
#include <assert.h>
#include <pthread.h>
#include  <signal.h>
#include <time.h>

int count = 0;

void flush_fifo(int flush_duration){
    
    printf("[Flush] Starting FIFO flush for %d seconds...\n", flush_duration);

    void *fifo_context = zmq_ctx_new ();
    assert(fifo_context != NULL);

    void *fifo_socket = zmq_socket (fifo_context, ZMQ_ROUTER);
    assert(fifo_socket != NULL);

    int check_bind = zmq_bind(fifo_socket, "tcp://*:5555");
    if (check_bind != 0) {
        printf("Bind Error: %s\n", zmq_strerror(zmq_errno()));
        zmq_close(fifo_socket);
        zmq_ctx_destroy(fifo_context);
        return;
    }

    printf("Flushing FIFO binded on port 5555\n");


    int more_msg;
    size_t more_msg_size = sizeof(more_msg);

    time_t start = time(NULL);

    while((time(NULL) - start) < flush_duration){

        
        do{

            zmq_msg_t part;
            int check = zmq_msg_init (&part);
            assert(check == 0);

            check = zmq_msg_recv (&part, fifo_socket, 0);
            if (check == -1) {
                printf("Receive Error: %s\n", zmq_strerror(zmq_errno()));
                break;  
            }
            


            check = zmq_getsockopt (fifo_socket, ZMQ_RCVMORE, &more_msg, &more_msg_size);
            assert(check == 0);

            zmq_msg_close (&part);


        }while (more_msg);



    }

    zmq_close(fifo_socket);
    zmq_ctx_destroy(fifo_context);
    count = 0;
    printf("[Flush] Finished\n");
    return;
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        printf("Usage: %s <flush_duration_seconds>\n", argv[0]);
        return 1;
    }

    int flush_duration = atoi(argv[1]);
    if (flush_duration <= 0) {
        printf("Invalid duration: %s\n", argv[1]);
        return 1;
    }

    flush_fifo(flush_duration);
    return 0;
}

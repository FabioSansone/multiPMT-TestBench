#include <stdio.h>
#include <string.h>
#include <zmq.h>
#include <assert.h>
#include <pthread.h>
#include  <signal.h>
#include <time.h>





#define BUFFER_SIZE 8  //8 parole da 16 bit => dimensione della parola dal DMA

static volatile sig_atomic_t keep_running = 1;

uint16_t shared_buffer[BUFFER_SIZE];
uint16_t fifo_buffer[BUFFER_SIZE];
uint16_t cut_shared_buffer[BUFFER_SIZE-2];
pthread_mutex_t lock;
pthread_cond_t buffer_ready = PTHREAD_COND_INITIALIZER;
int count = 0;


static void sig_handler(int _)
{
    (void)_;
    keep_running = 0;
}

void cut_head_tail(const uint16_t *buffer) {
    size_t j = 0; 
    for (size_t i = 1; i < BUFFER_SIZE - 1; i++) {
        cut_shared_buffer[j++] = buffer[i];
    }
}

uint32_t get_bits(const uint16_t *buffer, size_t bit_offset, size_t num_bits){


    uint32_t result = 0;

    for(size_t i = 0; i < num_bits; i++){

        size_t current_bit = bit_offset + i;
        size_t word_index = current_bit / 16;
        //Con il modulo determino l'indice del bit all'interno della parola. E' come se riscalassi gli indici ad un intevrallo compreso tra 0 e 15 
        //Il 15 - mi serve per traslare visto che non posso leggere da 0 a 15 come se fosse una lista ma da 15 a 0 (prima MSB)
        size_t bit_in_word = 15 - (current_bit % 16);

        uint16_t  word_16 = buffer[word_index];
        //Data la parola sposto tutto a destra di quello che serve, conoscendo l'indice del bit nella lista, per poi prendermelo come ultimo elemento
        uint32_t  bit_value = (word_16 >> bit_in_word) & 1;

        result = (result << 1) | bit_value;


    }


    return result;

}

int check_crc(const uint16_t *buffer) {
    uint8_t crc_fpga = get_bits(buffer, 88, 8); 
    uint8_t crc_calc = 0;
 
    for (size_t byte_idx = 0; byte_idx < 11; byte_idx++) {
        uint8_t byte_val = get_bits(buffer, byte_idx * 8, 8);
        crc_calc ^= byte_val;
    }

    return (crc_fpga == crc_calc) ? 0 : 1;
}




void *run_control(void *args){
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);

    void *context_rc = zmq_ctx_new ();
    assert(context_rc != NULL);

    void *rc_socket = zmq_socket (context_rc, ZMQ_PUB);
    assert(rc_socket != NULL);

    int check_rc_bind = zmq_bind(rc_socket, "tcp://*:4444");
    if (check_rc_bind != 0){
        printf("Bind Error: %s\n", zmq_strerror(zmq_errno()));
        return NULL;
    }

    printf("RC binded on port 4444\n");

    zmq_send(rc_socket, "start", 5, 0);
    printf("Sent START message\n");
    
    while(keep_running){
        sleep(1);
    }

    zmq_send(rc_socket, "stop", 4, 0);
    printf("Sent STOP message\n");

    zmq_close(rc_socket);
    zmq_ctx_destroy(context_rc);
    return NULL;
}



void *receive_data(void *args){

    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);


    void *context = zmq_ctx_new ();
    assert(context != NULL);

    void *server_socket = zmq_socket (context, ZMQ_ROUTER);
    assert(server_socket != NULL);

    int check_bind = zmq_bind(server_socket, "tcp://*:5555");
    if (check_bind != 0) {
        printf("Bind Error: %s\n", zmq_strerror(zmq_errno()));
        return NULL;
    }

    printf("Server binded on port 5555\n");


    int more_msg;
    size_t more_msg_size = sizeof(more_msg);


    while(keep_running){
        
        pthread_testcancel();

        
        do{

            zmq_msg_t part;
            int check = zmq_msg_init (&part);
            assert(check == 0);

            check = zmq_msg_recv (&part, server_socket, 0);
            if (check == -1) {
                printf("Receive Error: %s\n", zmq_strerror(zmq_errno()));
                break;  
            }
            pthread_testcancel();


            size_t part_size = zmq_msg_size (&part);
            unsigned char *data = (unsigned char *) zmq_msg_data(&part);

            if(part_size > 1){

                size_t num_words = part_size / 2;
                
                pthread_mutex_lock(&lock);
                for(size_t i=0; i < num_words; i++){
                    if (count < BUFFER_SIZE){
                        //qui mi prendo due byte alla volta e li metto nello shared buffer. In realtà salto di due con i*2 ma prendo un pezzo grande quanto uint_16
                        memcpy(&shared_buffer[count], &data[i*2], sizeof(uint16_t)); 
                        count++;
                    }
                    
                    //printf("%04x ", value);

                    if (count == BUFFER_SIZE){
                        pthread_cond_signal(&buffer_ready);
                    }


                }
                pthread_mutex_unlock(&lock);


            }


            check = zmq_getsockopt (server_socket, ZMQ_RCVMORE, &more_msg, &more_msg_size);
            assert(check == 0);

            zmq_msg_close (&part);


        }while (more_msg);



    }



    zmq_close(server_socket);
    zmq_ctx_destroy(context);
    return NULL;


}


void *process_data(void *file_ptr_void) {

    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);


    FILE *file = (FILE *)file_ptr_void;

    while(keep_running) {
        pthread_testcancel();

        pthread_mutex_lock(&lock);

       
        while (count < BUFFER_SIZE) {
            pthread_cond_wait(&buffer_ready, &lock);

        }

        pthread_testcancel();

        cut_head_tail(shared_buffer);

        if(check_crc(cut_shared_buffer) == 0){
            uint32_t canale = get_bits(cut_shared_buffer, 3, 5);
            uint32_t tempo_16_bit = get_bits(cut_shared_buffer, 8, 16);
            uint32_t coarse_time = (get_bits(cut_shared_buffer, 24, 8) << 20) | (get_bits(cut_shared_buffer, 33, 7) << 13) | get_bits(cut_shared_buffer, 40, 13);
            uint32_t tot = get_bits(cut_shared_buffer, 53, 6);
            uint32_t tdc_trigger_end = get_bits(cut_shared_buffer, 59, 5);
            uint32_t tdc_time = get_bits(cut_shared_buffer, 69, 5);
            uint32_t energia = get_bits(cut_shared_buffer, 74, 14);
            //uint32_t crc = get_bits(cut_shared_buffer, 88, 8);

            fprintf(file, "%u,%u,%u,%u,%u,%u,%u\n",
                    canale, tempo_16_bit, coarse_time, tdc_time, tot, tdc_trigger_end, energia);
            fflush(file);
            
            /*
            printf("Channel: %u\n", canale);
            printf("Tempo 16 bit: %u\n", tempo_16_bit);
            printf("Coarse: %u\n", coarse_time);
            printf("ToT: %u\n", tot);
            printf("TDC trigger end: %u\n", tdc_trigger_end);
            printf("TDC time: %u\n", tdc_time);
            printf("Energy: %u\n", energia);
            printf("CRC: %u\n", crc); */
        }
        else {
            printf("CRC mismatch! Data ignored.\n");
        }
                
        

        
        count = 0;


        pthread_mutex_unlock(&lock);
    }
}

int main(int argc, char *argv[]){

    if(argc < 2){
        printf("Usage: %s output_file.csv\n", argv[0]);
        return 1;
    }

    FILE *fout = fopen(argv[1], "w");
    if (!fout) {
        perror("Error opening output file");
        return 1;
    }

    int duration = 0; // 0 = infinity
    if(argc >= 3){
        duration = atoi(argv[2]);
        if(duration<0) duration = 0; //fallback for invalid argument
    }

    signal(SIGINT, sig_handler);  

    fprintf(fout, "Channel,Unix_time_16_bit,Coarse_time,TDC_time,ToT_time,TDC_trigger_end,Energy,CRC\n");

    pthread_t receiver, processing, rc_thread;
    pthread_mutex_init(&lock, NULL);

    pthread_create(&rc_thread, NULL, run_control, NULL);
    pthread_create(&receiver, NULL, receive_data, NULL);
    pthread_create(&processing, NULL, process_data, fout);

    time_t start = time(NULL);

    while (keep_running) {
        puts("Running...");
        sleep(1);

        if (duration > 0){
            time_t now = time(NULL);
            if (difftime(now, start) >= duration){
                printf("Acquisition time (%d sec) elapsed, stop!\n", duration);
                keep_running = 0;
                break;
            }
        }
    }

    puts("Stopping threads...");


    pthread_join(receiver, NULL);
    pthread_join(processing, NULL);
    pthread_join(rc_thread, NULL);

    fclose(fout);
    puts("Stopped by signal `SIGINT'");
    return 0;
}


#include <stdlib.h>
#include <string.h>
#include <libgen.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <fcntl.h>
#include <string.h>
#include <ctype.h>
#include <time.h>
#include <libcircle.h>
#include <mpi.h>

#ifdef HAVE_PANFS_SDK
    #include <pan_fs_client_cw_mode.h>
    #define _OPEN_FLAGS O_CREAT | O_WRONLY | O_CONCURRENT_WRITE
#else
    #define _OPEN_FLAGS O_CREAT | O_WRONLY
#endif

#include "log.h"
#ifdef HAVE_SPLICE
#define SPLICE_F_MOVE 1 
#endif
#define MAX_TRIES 50
#define STRING_SIZE 4096
#define CHUNK_SIZE 33554432 // 32 MB
#define _DEF_BUFSIZE 33554432

FILE           *DCOPY_debug_stream;
DCOPY_loglevel DCOPY_debug_level;
int             CIRCLE_global_rank;
char           *TOP_DIR;
int             TOP_DIR_LEN;
char           *DEST_DIR;
typedef enum { COPY,CHECKSUM,STAT } operation_code_t;
char  *op_string_table[] = { "COPY", "CHECKSUM", "STAT"};
typedef struct 
{
   operation_code_t code;
   int chunk;
   int tries;
   char * operand;
} operation_t;

time_t time_started;
time_t time_finished;
size_t total_bytes_copied;
void (*jump_table[4])(operation_t * op,CIRCLE_handle *handle);

char * 
encode_operation(operation_code_t op, int chunk,int tries, char * operand)
{
    char * result = (char *) malloc(sizeof(char)*STRING_SIZE);
    sprintf(result,"%d:%d:%d:%s",chunk, op,tries,operand);
    return result;
}

void
do_checksum(operation_t * op, CIRCLE_handle * handle)
{
    LOG(DCOPY_LOG_DBG,"Checksum %s chunk %d",op->operand, op->chunk);
    char path[STRING_SIZE];
    sprintf(path,"%s/%s",TOP_DIR,op->operand);
    FILE * old = fopen(path,"rb");
    if(!old)
    {
        LOG(DCOPY_LOG_ERR,"Unable to open file %s",path);
        perror("checksum");
        return;
    }
    char newfile[STRING_SIZE];
    void * newbuf = (void*) malloc(CHUNK_SIZE);
    void * oldbuf = (void*) malloc(CHUNK_SIZE);
    sprintf(newfile,"%s/%s",DEST_DIR,op->operand);
    FILE * new = fopen(newfile,"rb");
    if(!new)
    {
        LOG(DCOPY_LOG_ERR,"Unable to open file %s",newfile);
        perror("checksum open");
        op->tries = op->tries + 1;
        char *newop = encode_operation(CHECKSUM,op->chunk,op->tries,op->operand);
        if(op->tries <= MAX_TRIES)
            handle->enqueue(newop);
        free(newop);
        return;
    }
    fseek(new,CHUNK_SIZE*op->chunk,SEEK_SET);
    fseek(old,CHUNK_SIZE*op->chunk,SEEK_SET);
    size_t newbytes = fread(newbuf,1,CHUNK_SIZE,new);
    size_t oldbytes = fread(oldbuf,1,CHUNK_SIZE,old);
    if(newbytes != oldbytes || memcmp(newbuf,oldbuf,newbytes) != 0)
    {
        LOG(DCOPY_LOG_ERR,"Incorrect checksum, requeueing file (%s).",op->operand);
        char *newop = encode_operation(STAT,0,0,op->operand);
        handle->enqueue(newop);
        free(newop);
    }
    else
        LOG(DCOPY_LOG_DBG,"File (%s) chunk %d OK.",newfile,op->chunk);
    if(new) fclose(new);
    if(old) fclose(old);
    free(newbuf);
    free(oldbuf);
    return;
}

    
void 
process_dir(char * dir, CIRCLE_handle *handle)
{
    DIR *current_dir;
    char parent[STRING_SIZE];
    struct dirent *current_ent;
    char path[STRING_SIZE];
    int is_top_dir = !strcmp(dir,TOP_DIR);
    if(is_top_dir)
        sprintf(path,"%s",dir);
    else
        sprintf(path,"%s/%s",TOP_DIR,dir);
    current_dir = opendir(path);
    if(!current_dir) 
    {
        LOG(DCOPY_LOG_ERR, "Unable to open dir: %s",path);
    }
    else
    {
        /* Read in each directory entry */
        while((current_ent = readdir(current_dir)) != NULL)
        {
            /* We don't care about . or .. */
            if((strncmp(current_ent->d_name,".",2)) && (strncmp(current_ent->d_name,"..",3)))
            {
                LOG(DCOPY_LOG_DBG,"Dir entry %s / %s",dir,current_ent->d_name);
                if(is_top_dir)
                    strcpy(parent,"");
                else
                    strcpy(parent,dir);
                
                LOG(DCOPY_LOG_DBG,"Parent %s",parent);
                strcat(parent,"/");
                strcat(parent,current_ent->d_name);
                LOG(DCOPY_LOG_DBG, "Pushing [%s] <- [%s]", parent, dir);
                char *newop = encode_operation(STAT,0,0,parent);
                handle->enqueue(newop);
                free(newop);
            }
        }
    }
    closedir(current_dir);
    return;
}

void
do_stat(operation_t * op, CIRCLE_handle * handle)
{
    static struct stat st;
    static int status;
    int is_top_dir = !strcmp(op->operand,TOP_DIR);
    static char path[STRING_SIZE];
    int temp;
    if(is_top_dir)
        sprintf(path,"%s",TOP_DIR);
    else
        sprintf(path,"%s/%s",TOP_DIR,op->operand);
    status = lstat(path, &st);
    if(status != EXIT_SUCCESS)
    {
        LOG(DCOPY_LOG_ERR,"Unable to stat \"%s\"",path);
        perror("stat");
    }
    else if(S_ISDIR(st.st_mode) && !(S_ISLNK(st.st_mode)))
    {
          char dir[STRING_SIZE];
          LOG(DCOPY_LOG_DBG,"Operand: %s Dir: %s",op->operand,DEST_DIR);
          if(is_top_dir)
              sprintf(dir,"mkdir -p %s",op->operand);
          else
              sprintf(dir,"mkdir -p %s/%s",DEST_DIR,op->operand);
          LOG(DCOPY_LOG_DBG,"Creating %s",dir);
          FILE * p = popen(dir,"r");
          if(!p) perror("Unable to mkdir");
          else
              pclose(p);
          if(chown(dir,st.st_uid,st.st_gid) != EXIT_SUCCESS)
          {
              LOG(DCOPY_LOG_ERR,"Unable to chown (%s)",dir);
              perror("chown");
          }
          process_dir(op->operand,handle);
    }
    else
    {
          sprintf(path,"%s/%s",DEST_DIR,op->operand);
          temp = creat(path,st.st_mode);
          if(temp == -1)
          {
              LOG(DCOPY_LOG_ERR,"Unable to creat (%s)",path);
              perror("creat");
          }
          if(fchown(temp,st.st_uid,st.st_gid) != EXIT_SUCCESS)
          {
              LOG(DCOPY_LOG_ERR,"Unable to fchown (%s)",path);
              perror("fchown");
          }
          close(temp);
          int num_chunks = st.st_size / CHUNK_SIZE;
          LOG(DCOPY_LOG_DBG,"File size: %ld Chunks:%d Total: %d",st.st_size,num_chunks,num_chunks*CHUNK_SIZE);
          int i = 0;
          if(num_chunks == 0)
          {
             // For a single chunk, just copy it.
             do_copy(op,handle);  
          }
          else
          {
              for(i = 0; i < num_chunks; i++)
              {
                 char *newop = encode_operation(COPY,i,0,op->operand);
                 handle->enqueue(newop);
                 free(newop);
              }
              if(num_chunks*CHUNK_SIZE < st.st_size)
              {
                 char *newop = encode_operation(COPY,i,0,op->operand);
                 handle->enqueue(newop);
                 free(newop);
              }
          }
    }
    return;
}
#ifdef HAVE_SPLICE
void
do_splice_copy(operation_t * op, CIRCLE_handle * handle)
{
    LOG(DCOPY_LOG_DBG,"Spliced Copy %s chunk %d",op->operand, op->chunk);
    char path[STRING_SIZE];
    char newfile[STRING_SIZE];
    size_t bytes = 0;
    static int buf_size = _DEF_BUFSIZE;
    int in, out;
    off_t bytes_left = 0;
    int splice_des[2];
    sprintf(path,"%s/%s",TOP_DIR,op->operand);
    in = open(path,O_RDONLY);
    if(!in)
    {
        LOG(DCOPY_LOG_ERR,"Unable to open %s",path);
        perror("open");
        return;
    }
    sprintf(newfile,"%s/%s",DEST_DIR,op->operand);
    out = open(newfile,_OPEN_FLAGS);
    if(!out)
    {
        LOG(DCOPY_LOG_WARN,"Warning: file (%s) doesn't have attributes set.",newfile);
        if(!out)
        {
            LOG(DCOPY_LOG_ERR,"Unable to open %s",newfile);
            perror("destination");
            close(in);
            return;
        }
    }
    if(lseek(in,(off_t)(CHUNK_SIZE*op->chunk),SEEK_SET) != (off_t) (CHUNK_SIZE*op->chunk))
    {
        LOG(DCOPY_LOG_ERR,"Couldn't lseek %s",op->operand);
        perror("lseek");
        close(in);
        close(out);
        return;
    }
        LOG(DCOPY_LOG_DBG,"Read %ld bytes.",bytes);
    if(lseek(out,(off_t)(CHUNK_SIZE*op->chunk),SEEK_SET) != (off_t) (CHUNK_SIZE*op->chunk))
    {
        LOG(DCOPY_LOG_ERR,"Unable to seek to %d in %s",CHUNK_SIZE*op->chunk,newfile);
        perror("lseek");
        close(in);
        close(out);
        return;
    }
    if(pipe(splice_des) < 0)
    {
        perror("pipe");
        close(in);
        close(out);
        return;
    }
    bytes_left = CHUNK_SIZE;
    while(bytes_left > 0)
    {
        //long splice(int fd_in, off_t *off_in, int fd_out,  off_t *off_out, size_t len, unsigned int flags);
        if(splice(in, 0, splice_des[1], 0, buf_size, SPLICE_F_MOVE) < 0)
        {
            LOG(DCOPY_LOG_ERR,"Splice failed copying into a pipe.");
            perror("splice");
            close(in);
            close(out);
            return;
        }
        if(splice(splice_des[0], NULL, out, NULL, buf_size, SPLICE_F_MOVE) < 0)
        {
            LOG(DCOPY_LOG_ERR,"Splice failed copying into a fd.");
            perror("splice");
            close(in);
            close(out);
            return;
        }
        bytes_left -= buf_size;
    }
    total_bytes_copied += CHUNK_SIZE;
    LOG(DCOPY_LOG_DBG,"Wrote %zd bytes (%zd total).",bytes,total_bytes_copied);
    close(in);
    close(out);
    return;
}
#endif

void
do_copy(operation_t * op, CIRCLE_handle * handle)
{
    LOG(DCOPY_LOG_DBG,"Copy %s chunk %d",op->operand, op->chunk);
    char path[STRING_SIZE];
    char newfile[STRING_SIZE];
    char buf[CHUNK_SIZE];
    size_t qty = 0;
    FILE * in;
    int out;
    sprintf(path,"%s/%s",TOP_DIR,op->operand);
    in = fopen(path,"rb");
    if(!in)
    {
        LOG(DCOPY_LOG_ERR,"Unable to open %s",path);
        perror("open");
        return;
    }
    sprintf(newfile,"%s/%s",DEST_DIR,op->operand);
    out = open(newfile,_OPEN_FLAGS);
    if(!out)
    {
        LOG(DCOPY_LOG_WARN,"Warning: file (%s) doesn't have attributes set.",newfile);
        if(!out)
        {
            LOG(DCOPY_LOG_ERR,"Unable to open %s",newfile);
            perror("destination");
            fclose(in);
            return;
        }
    }
    if(fseek(in,CHUNK_SIZE*op->chunk,SEEK_SET) != 0)
    {
        LOG(DCOPY_LOG_ERR,"Couldn't seek %s",op->operand);
        perror("fseek");
        fclose(in);
        close(out);
        return;
    }
    size_t bytes = fread((void*)buf,1,CHUNK_SIZE,in);
    if((int)bytes == 0 && ferror(in))
    {
        LOG(DCOPY_LOG_ERR,"Warning: read %s (bytes = %zu)",op->operand,bytes);
        fclose(in);
        close(out);
        return;
    }
    LOG(DCOPY_LOG_DBG,"Read %ld bytes.",bytes);
    if(lseek(out,(off_t)(CHUNK_SIZE*op->chunk),SEEK_SET) != (off_t) (CHUNK_SIZE*op->chunk))
    {
        LOG(DCOPY_LOG_ERR,"Unable to seek to %d in %s",CHUNK_SIZE*op->chunk,newfile);
        perror("lseek");
        fclose(in);
        close(out);
        return;
    }
    qty = write(out,buf,bytes);
    total_bytes_copied += bytes;
    LOG(DCOPY_LOG_DBG,"Wrote %zd bytes (%zd total).",bytes,total_bytes_copied);
    fclose(in);
    close(out);
    return;
}

operation_t *
decode_operation(char * op)
{
    operation_t * ret = (operation_t*) malloc(sizeof(operation_t));
    ret->operand = (char*) malloc(sizeof(char)*STRING_SIZE);
    ret->chunk = atoi(strtok(op,":"));
    ret->code = atoi(strtok(NULL,":"));
    ret->tries = atoi(strtok(NULL,":"));
    ret->operand = strtok(NULL,":"); 
    return ret;
}

void
add_objects(CIRCLE_handle *handle)
{
    TOP_DIR_LEN = strlen(TOP_DIR);
    char * op = encode_operation(STAT,0,0,TOP_DIR);
    handle->enqueue(op);
    free(op);
}


void
process_objects(CIRCLE_handle *handle)
{
    char op[STRING_SIZE]; 
    /* Pop an item off the queue */ 
    LOG(DCOPY_LOG_DBG, "Popping, queue has %d elements", handle->local_queue_size());
    handle->dequeue(op);
    operation_t * opt = decode_operation(op);
    LOG(DCOPY_LOG_DBG, "Popped [%s]",opt->operand);
    LOG(DCOPY_LOG_DBG, "Operation: %d %s",opt->code,op_string_table[opt->code]);
    jump_table[opt->code](opt,handle);
    free(opt);
    return;
}


int
main (int argc, char **argv)
{
    int c;

    jump_table[0] = do_copy;
    jump_table[1] = do_checksum;
    jump_table[2] = do_stat;
    total_bytes_copied = 0.0;
    
    DCOPY_debug_stream = stdout;
    DCOPY_debug_level = DCOPY_LOG_DBG;

    int CIRCLE_global_rank = CIRCLE_init(argc, argv);
    opterr = 0;

    TOP_DIR = DEST_DIR = NULL;
    while((c = getopt(argc, argv, "l:s:d:")) != -1)
    {
        switch(c)
        {
            case 's':
                TOP_DIR = optarg;
                break;
            case 'd':
                DEST_DIR = optarg;
                break;
            case 'l':
                DCOPY_debug_level = atoi(optarg);
                break;

            case '?':
                if (optopt == 'l')
                {
                    fprintf(stderr, "Option -%c requires an argument.\n", optopt);
                    exit(EXIT_FAILURE);
                }
                else if (isprint (optopt))
                {
                    fprintf(stderr, "Unknown option `-%c'.\n", optopt);
                    exit(EXIT_FAILURE);
                }
                else
                {
                    fprintf(stderr,
                        "Unknown option character `\\x%x'.\n",
                        optopt);
                    exit(EXIT_FAILURE);
                }

            default:
                abort();
        }
    }
    if((TOP_DIR == NULL || DEST_DIR == NULL) && CIRCLE_global_rank == 0)
    {
        LOG(DCOPY_LOG_ERR,"You must specify a source and destination path.");
        exit(1);
    }
    time(&time_started);
    CIRCLE_cb_create(&add_objects);
    CIRCLE_cb_process(&process_objects);
    double start = MPI_Wtime();
    CIRCLE_begin();
    double end = MPI_Wtime() - start;
    CIRCLE_finalize();
    time(&time_finished);
    char starttime_str[256];
    char endtime_str[256];
    struct tm * localstart = localtime( &time_started );
    struct tm * localend = localtime ( &time_finished );
    strftime(starttime_str, 256, "%b-%d-%Y,%H:%M:%S",localstart);
    strftime(endtime_str, 256, "%b-%d-%Y,%H:%M:%S",localend);
        LOG(DCOPY_LOG_INFO, "Filecopy run started at: %s", starttime_str);
        LOG(DCOPY_LOG_INFO, "Filecopy run completed at: %s", endtime_str);
        LOG(DCOPY_LOG_INFO, "Filecopy total time (seconds) for this run: %f",difftime(time_finished,time_started));
        LOG(DCOPY_LOG_INFO, "Transfer rate: %zd bytes in %lf seconds (%f MB/s).",total_bytes_copied,end,(float)total_bytes_copied/(float)end/1024.0/1024.0);
    exit(EXIT_SUCCESS);
}

/* EOF */

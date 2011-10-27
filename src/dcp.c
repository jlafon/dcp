#include <stdlib.h>
#include <string.h>
#include <libgen.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <string.h>
#include <ctype.h>
#include <time.h>
#include <libcircle.h>
#include <mpi.h>
#include "log.h"
#define STRING_SIZE 4096
#define CHUNK_SIZE 4194304
FILE           *DCOPY_debug_stream;
DCOPY_loglevel DCOPY_debug_level;
int             CIRCLE_global_rank;
char           *TOP_DIR;
int             TOP_DIR_LEN;
char           *DEST_DIR;
typedef enum { COPY,CHECKSUM,STAT,SETATTR } operation_code_t;
char  *op_string_table[] = { "COPY", "CHECKSUM", "STAT", "SETATTR" };

typedef struct 
{
   operation_code_t code;
   int chunk;
   char * operand;
} operation_t;

time_t time_started;
time_t time_finished;
size_t total_bytes_copied;
void (*jump_table[4])(operation_t * op,CIRCLE_handle *handle);

char * 
encode_operation(operation_code_t op, int chunk,char * operand)
{
    char * result = (char *) malloc(sizeof(char)*STRING_SIZE);
    sprintf(result,"%d:%d:%s",chunk, op,operand);
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
        char *newop = encode_operation(CHECKSUM,op->chunk,op->operand);
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
        char *newop = encode_operation(STAT,0,op->operand);
        handle->enqueue(newop);
        free(newop);
    }
    else
        LOG(DCOPY_LOG_DBG,"File (%s) chunk %d OK.",newfile,op->chunk);
    fclose(new);
    fclose(old);
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
                char *newop = encode_operation(STAT,0,parent);
                handle->enqueue(newop);
                free(newop);
            }
        }
    }
    closedir(current_dir);
    return;
}

void
do_setattr(operation_t * op, CIRCLE_handle * handle)
{
    static struct stat st;
    static int status;
    int is_top_dir = !strcmp(op->operand,TOP_DIR);
    char path[STRING_SIZE];
    char destpath[STRING_SIZE];
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
    LOG(DCOPY_LOG_DBG,"Operand: %s Dir: %s",op->operand,DEST_DIR);
    if(is_top_dir)
    {
        if(chmod(op->operand,st.st_mode) != 0)
        {
            LOG(DCOPY_LOG_ERR,"Unable to chmod %s to %d",op->operand,st.st_mode);
            perror("chmod");
        }
    }
    else
    {
        sprintf(destpath,"%s/%s",DEST_DIR,op->operand);
        if(chmod(destpath,st.st_mode) != 0)
        {
            LOG(DCOPY_LOG_ERR,"Unable to chmod %s to %d",destpath,st.st_mode);
            perror("chmod");
        }
        LOG(DCOPY_LOG_DBG,"Chmod'd %s to %d",destpath,st.st_mode);
    }
    return;
}

void
do_stat(operation_t * op, CIRCLE_handle * handle)
{
    static struct stat st;
    static int status;
    int is_top_dir = !strcmp(op->operand,TOP_DIR);
    char path[STRING_SIZE];
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
          char dir[2048];
          LOG(DCOPY_LOG_DBG,"Operand: %s Dir: %s",op->operand,DEST_DIR);
          if(is_top_dir)
              sprintf(dir,"mkdir -p %s",op->operand);
          else
              sprintf(dir,"mkdir -p %s/%s",DEST_DIR,op->operand);
          LOG(DCOPY_LOG_DBG,"Creating %s",dir);
          FILE * p = popen(dir,"r");
          pclose(p);
          process_dir(op->operand,handle);
    }
    else
    {
          int num_chunks = st.st_size / CHUNK_SIZE;
          LOG(DCOPY_LOG_DBG,"File size: %ld Chunks:%d Total: %d",st.st_size,num_chunks,num_chunks*CHUNK_SIZE);
          int i = 0;
          for(i = 0; i < num_chunks; i++)
          {
             char *newop = encode_operation(COPY,i,op->operand);
             handle->enqueue(newop);
             free(newop);
          }
          if(num_chunks*CHUNK_SIZE < st.st_size)
          {
             char *newop = encode_operation(COPY,i,op->operand);
             handle->enqueue(newop);
             char *attrop = encode_operation(SETATTR,0,op->operand);
             handle->enqueue(attrop);
             free(newop);
          }
    }
    return;
}

void
do_copy(operation_t * op, CIRCLE_handle * handle)
{
    LOG(DCOPY_LOG_DBG,"Copy %s chunk %d",op->operand, op->chunk);
    char path[STRING_SIZE];
    char newfile[STRING_SIZE];
    char buf[CHUNK_SIZE];
    int qty = 0;
    FILE * in, *out;
    sprintf(path,"%s/%s",TOP_DIR,op->operand);
    in = fopen(path,"rb");
    if(!in)
    {
        LOG(DCOPY_LOG_ERR,"Unable to open %s",path);
        perror("open");
        return;
    }
    sprintf(newfile,"%s/%s",DEST_DIR,op->operand);
    out = fopen(newfile,"rb+");
    if(!out)
    {
        LOG(DCOPY_LOG_ERR,"Unable to open %s",newfile);
        return;
    }
    if(fseek(in,CHUNK_SIZE*op->chunk,SEEK_SET) != 0)
    {
        LOG(DCOPY_LOG_ERR,"Couldn't seek %s",op->operand);
        perror("fseek");
        return;
    }
    size_t bytes = fread((void*)buf,1,CHUNK_SIZE,in);
    if(bytes <= 0)
    {
        LOG(DCOPY_LOG_ERR,"Couldn't read %s",op->operand);
        perror("fread");
        return;
    }
    LOG(DCOPY_LOG_DBG,"Read %ld bytes.",bytes);
    if(fseek(out,CHUNK_SIZE*op->chunk,SEEK_SET) != 0)
    {
        LOG(DCOPY_LOG_ERR,"Unable to seek to %d in %s",CHUNK_SIZE*op->chunk,newfile);
        perror("fseek");
        return;
    }
    qty = fwrite(buf,bytes,1,out);
    if(qty > 0) total_bytes_copied += qty;
    LOG(DCOPY_LOG_DBG,"Wrote %ld bytes (%ld total).",bytes,total_bytes_copied);
    char *newop = encode_operation(CHECKSUM,op->chunk,op->operand);
    handle->enqueue(newop);
    free(newop);
    fclose(in);
    fclose(out);
    return;
}

operation_t *
decode_operation(char * op)
{
    operation_t * ret = (operation_t*) malloc(sizeof(operation_t));
    ret->operand = (char*) malloc(sizeof(char)*STRING_SIZE);
    ret->chunk = atoi(strtok(op,":"));
    ret->code = atoi(strtok(NULL,":"));
    ret->operand = strtok(NULL,":"); 
    return ret;
}

void
add_objects(CIRCLE_handle *handle)
{
    TOP_DIR_LEN = strlen(TOP_DIR);
    char * op = encode_operation(STAT,0,TOP_DIR);
    handle->enqueue(op);
    free(op);
}


void
process_objects(CIRCLE_handle *handle)
{
    char op[2048]; 
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
    jump_table[3] = do_setattr;
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
        LOG(DCOPY_LOG_INFO, "Transfer rate: %ld bytes in %lf seconds.",total_bytes_copied,end);
    exit(EXIT_SUCCESS);
}

/* EOF */

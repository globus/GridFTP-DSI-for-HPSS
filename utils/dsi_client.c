/*
 * System includes.
 */
#include <sys/types.h>
#include <sys/stat.h>
#include <pthread.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <math.h>
#include <pwd.h>

/*
 * Globus includes.
 */
#include <globus_gridftp_server.h>

const int DefaultConcurrency = 4;
const int DefaultBlockSize   = (1024*1024);
const globus_off_t DefaultTransferLength = -1; // Entire file

/*
 * HPSS DSI module entry point.
 */
extern globus_gfs_storage_iface_t hpss_local_dsi_iface;

/*
 * Opaque Globus struct that we will redefine for our needs
 */
struct globus_l_gfs_data_operation_s {
	pthread_mutex_t lock;
	pthread_cond_t  cond;

	//
	// Client's callback arg, supplied through the DSI's init_func()
	//
	void * session_arg;
	// Result from initialization routine
	globus_result_t init_result;

	//
	// stat_func() state information.
	//
	struct dsi_stat {
		int finished;

		struct stat_chain {
			int                 stat_count;
			globus_gfs_stat_t * stat_array;
			globus_result_t     result;
			struct stat_chain * next;
		} * chain;
	} stat;

	//
	// recv_func() state information.
	//
	struct dsi_recv {
		int             block_size;  // bytes per buffer
		int             concurrency; // number of buffers
		int             finished;
		globus_result_t result;

		// ranges to send, typically (0, -1) [entire file]
		struct range {
			globus_off_t   offset;
			globus_off_t   length;
			struct range * next;
		} * ranges;

		// STOR operation
		struct buffer {
			globus_byte_t *                 buffer;
			globus_size_t                   length;
			globus_gridftp_server_read_cb_t callback;
			void *                          user_arg;
			struct buffer                 * next;
		} * buffers;
	} recv;
};

static void
op_init(globus_gfs_operation_t op)
{
	memset(op, 0, sizeof(*op));
	pthread_mutex_init(&op->lock, NULL);
	pthread_cond_init(&op->cond, NULL);
}

static void
op_destroy(globus_gfs_operation_t op)
{
	pthread_mutex_destroy(&op->lock);
	pthread_cond_destroy(&op->cond);
}

/*********************************************************************
 * Utility functions.
 ********************************************************************/
static char *
safe_strdup(const char *s)
{
	if (!s) return NULL;
	return strdup(s);
}

static void
print_result(globus_result_t Result)
{
	globus_object_t * o = globus_error_peek(Result);

	globus_object_printable_string_func_t func;
	func = globus_object_printable_get_string_func(o);

	printf("%s\n", func(o));
}

/*********************************************************************
 * Parsing highest level options.
 ********************************************************************/
struct global_options {
	int debug;
};

static int
parse_global_options(const char            * progname, 
                     int                     argc, 
                     char                  * argv[], 
                     struct global_options * go)
{
	char * usage = "[global_options] <cmd> [options] [args]\n"
	               "\t-d\t print debug information to stdout\n"
	               "\t-h\t print this help message and exit\n"
	               "Supported commands:\n"
	               "\tstat <path>\n"
	               "\tstor <src> <dst>\n";

	memset(go, 0, sizeof(*go));

	int opt;
	while ((opt = getopt(argc, argv, "+dh")) != -1)
	{
		switch (opt)
		{
		case 'd':
			go->debug = 1;
			break;
		case 'h':
			printf("Usage: %s %s", progname, usage);
			return 1;
		default:
			return 1;
		}
	}

	return 0;
}

/*********************************************************************
 * Interpret command given on the command line
 ********************************************************************/
typedef enum { UNKNOWN, STAT, STOR } cmd_t;

static cmd_t
parse_command(const char * cmd)
{
	if (cmd && strcmp(cmd, "stat") == 0)
		return STAT;
	if (cmd && strcmp(cmd, "stor") == 0)
		return STOR;
	return UNKNOWN;
}

/*********************************************************************
 * Parsing 'stat' command options.
 ********************************************************************/
struct stat_options {
	char * path;
};

int
parse_stat_options(int argc, char * argv[], struct stat_options * so)
{
	if (argc == 1)
		return 0;

	so->path = argv[1];
	return 0;
}

static int
stat_path(globus_gfs_operation_t   OP, 
          struct global_options  * GO,
          int                      argc,
          char                   * argv[])
{
	/* Parse our command-specific options. */
	struct stat_options so;
	int retval = parse_stat_options(argc, argv, &so);
	if (retval)
		return retval;

	/* Fill out the Globus stat info structure. */
	globus_gfs_stat_info_t stat_info;
	stat_info.file_only         = GLOBUS_FALSE;
	stat_info.pathname          = so.path;
	stat_info.use_symlink_info  = GLOBUS_FALSE;
	stat_info.include_path_stat = GLOBUS_FALSE;

	/* Launch the operation. */
	hpss_local_dsi_iface.stat_func(OP, &stat_info, OP->session_arg);

	/* Process the results. */
	pthread_mutex_lock(&OP->lock);
	{
		int finished = 0;

		while (!finished)
		{
			while (!OP->stat.finished && !OP->stat.chain)
				pthread_cond_wait(&OP->cond, &OP->lock);

			while (OP->stat.chain)
			{
				// Unlink this next array set
				struct stat_chain * sc = OP->stat.chain;
				OP->stat.chain = sc->next;

				// Print all names in this array
				for (int i = 0; i < sc->stat_count; i++)
				{
					printf("%s\n", sc->stat_array[i].name);
				}

				if (sc->result != GLOBUS_SUCCESS)
					print_result(sc->result);

				// Release the array contents
				for (int i = 0; i < sc->stat_count; i++)
				{
					free(sc->stat_array[i].name);
					free(sc->stat_array[i].symlink_target);
				}
				free(sc->stat_array);
				free(sc);
			}

			// Check if we are finished
			if (!finished) finished = OP->stat.finished;

		}
	}
	pthread_mutex_unlock(&OP->lock);
}

/*********************************************************************
 * Parsing 'stor' command options.
 ********************************************************************/
struct stor_options {
	int concurrency; // The number of concurrent buffers
	int blocksize;   // The size of the buffers
	globus_off_t length; // Length of the file to copy in

	char * src; // file to copy into HPSS
	char * dst; // target path within HPSS
};

int
parse_stor_options(int argc, char * argv[], struct stor_options * so)
{
	memset(so, 0, sizeof(*so));

	so->concurrency = DefaultConcurrency;
	so->blocksize   = DefaultBlockSize;
	so->length      = DefaultTransferLength;

	int opt;
	while ((opt = getopt(argc, argv, "+b:c:l:")) != -1)
	{
		switch (opt)
		{
		case 'b':
			so->blocksize = atoi(optarg);
			break;
		case 'c':
			so->concurrency = atoi(optarg);
			break;
		case 'l':
			so->length = atoll(optarg);
			break;
		default:
			return 1;
		}
	}

	so->src = safe_strdup(argv[optind]);
	if (!so->src)
	{
		printf("Missing <src>\n");
		return 1;
	}

	so->dst = safe_strdup(argv[optind + 1]);
	if (!so->dst)
	{
		printf("Missing <dst>\n");
		return 1;
	}

	return 0;
}

static int
stor_file(globus_gfs_operation_t        OP,
          const struct global_options * GO,
          int                           argc,
          char                        * argv[])
{
	/* Parse our command-specific options. */
	struct stor_options so;
	int retval = parse_stor_options(argc, argv, &so);
	if (retval)
		return 1;

	/* Determine the length of the src. */
	struct stat statbuf;
	retval = stat(so.src, &statbuf);
	if (retval == -1)
	{
		fprintf(stderr, "Failed to stat %s: %s\n", so.dst, strerror(errno));
		return 1;
	}

	/* Determine the length of this transfer. */
	globus_off_t transfer_length = 0;

	switch (statbuf.st_mode & S_IFMT)
	{
	case S_IFREG:
		transfer_length = so.length;
		if (so.length == DefaultTransferLength || so.length > statbuf.st_size)
			transfer_length = statbuf.st_size;
		break;

	default:
		if (so.length == DefaultTransferLength)
		{
			fprintf(stderr, "Specify transfer length using -l for non regular files\n");
			return 1;
		}
		transfer_length = so.length;
		break;
	}

	int fd = open(so.src, O_RDONLY);
	if (fd == -1)
	{
		fprintf(stderr, "Failed to open %s: %s\n", so.src, strerror(errno));
		return 1;
	}

	//
	// DSI's transfer_info
	//
	globus_gfs_transfer_info_t transfer_info;
	memset(&transfer_info, 0, sizeof(transfer_info));
	// Destination path
	transfer_info.pathname = so.dst;
	// Length of incoming data
	transfer_info.alloc_size = transfer_length;

	//
	// Our control structure
	//
	OP->recv.block_size  = so.blocksize;
	OP->recv.concurrency = so.concurrency;

	// Insert our transfer ranges
	//   (0,-1)   : complete file
	//   (eof,-1) : transfer complete
	struct range * r = calloc(1, sizeof(*r));
	r->offset = 0;
	r->length = -1;
	r->next   = calloc(1, sizeof(*r));
	r->next->offset = transfer_info.alloc_size;
	r->next->length = -1;
	OP->recv.ranges = r;

	/* Start out timer. */
	struct timeval t_begin;
	gettimeofday(&t_begin, NULL);

	hpss_local_dsi_iface.recv_func(OP, &transfer_info, OP->session_arg);

	globus_off_t transfer_offset = 0;
	int finished = 0;
	while (!finished)
	{
		struct buffer * buffers = NULL;
		pthread_mutex_lock(&OP->lock);
		{
			do {
				// Copy out all buffers
				buffers = OP->recv.buffers;
				OP->recv.buffers = NULL;

				// Indicate finished only when no buffers left
				if (!buffers && OP->recv.finished)
					finished = 1;

				if (!buffers && !OP->recv.finished)
					pthread_cond_wait(&OP->cond, &OP->lock);

			} while (!buffers && !OP->recv.finished);
		}
		pthread_mutex_unlock(&OP->lock);

		while (buffers)
		{
			struct buffer * r = buffers;
			buffers = r->next;

			/* Assume we'll read the length of the supplied buffer. */
			globus_off_t buffer_length = r->length;
			/* Truncate to the lessor of the buffer length and remaining byte count. */
			if (buffer_length > (transfer_length - transfer_offset))
				buffer_length = (transfer_length - transfer_offset);

			if (buffer_length > 0)
				buffer_length = pread(fd, r->buffer, buffer_length, transfer_offset);

			int eof = ((transfer_offset+buffer_length) == transfer_length);

			if (GO->debug && eof)
			{
				printf("EOF\n");
				fflush(stdout);
			}

			// Call the user callback
			r->callback(OP, GLOBUS_SUCCESS, r->buffer, buffer_length, transfer_offset, eof, r->user_arg);
			transfer_offset += buffer_length;
			free(r);
		}
	}

	/* Stop the timer. */
	struct timeval t_end;
	gettimeofday(&t_end, NULL);

	/* Report our transfer rante. */
	time_t usecs = (t_end.tv_sec - t_begin.tv_sec) * pow(10,6) + (10^-t_begin.tv_usec) + (t_end.tv_usec);
	double rate = ((double)transfer_length) / usecs * pow(10,6);

	printf("Transferred %llu bytes in %f seconds.", transfer_length, ((float)usecs)/pow(10,6));

	if (rate < 1024)
		printf(" %lf B/s\n", rate);
	else if (rate < (1024 * 1024))
		printf(" %lf KiB/s\n", rate/1024);
	else if (rate < (1024 * 1024 * 1024))
		printf(" %lf MiB/s\n", rate/(1024 * 1024));
	else 
		printf(" %lf GiB/s\n", rate/(1024 * 1024 * 1024));

	/* Close the source */
	close(fd);

	return 0;
}

int
main(int argc, char * argv[])
{
	// Initialize the Globus error module so we can decode GridFTP errors
	globus_module_activate(&globus_i_error_module);

	/*
	 * Parse global options.
	 */
	struct global_options global_options;
	int retval = parse_global_options(argv[0], argc, argv, &global_options);
	if (retval)
		return 1;

	/*
	 * Initialize our operations callback arg.
	 */
	struct globus_l_gfs_data_operation_s op;
	op_init(&op);

	/* Determine our user name. */
	struct passwd * pw = getpwuid(geteuid());
	char * username = safe_strdup(pw->pw_name);

	/* Authenticate to HPSS as 'username' */
	globus_gfs_session_info_t session_info = {.username = username};
	hpss_local_dsi_iface.init_func(&op, &session_info);

	if (op.init_result != GLOBUS_SUCCESS)
	{
		print_result(op.init_result);
		return 1;
	}

	cmd_t c = parse_command(argv[optind]);
	switch (c)
	{
	case STAT:
		retval = stat_path(&op, &global_options, argc-optind, &argv[optind]);
		break;
	case STOR:
		retval = stor_file(&op, &global_options, argc-optind, &argv[optind]);
		break;
	default:
		printf("missing <cmd>. Try '%s -h' for more information\n", argv[0]);
		return 1;
	}


cleanup:
	free(username);
	op_destroy(&op);
	return 0;
}

////////////////////////////////////////////////////////////////////////////////
//
// Our mock routines. Thin stubs with logic processing and reporiting in the
// higher-level control logic.
//
////////////////////////////////////////////////////////////////////////////////

/**********************************************************
 *
 *  DSI init_func() callbacks
 *
 *********************************************************/
globus_result_t
globus_gridftp_server_add_command(
    globus_gfs_operation_t              op,
    const char *                        command_name,
    int                                 cmd_id,
    int                                 min_args,
    int                                 max_args,
    const char *                        help_string,
    globus_bool_t                       has_pathname,
    int                                 access_type)
{
	/*
	 * We currently do nothing with the added commands.
	 */
	return GLOBUS_SUCCESS;
}

void
globus_gridftp_server_finished_session_start(
    globus_gfs_operation_t              op,
    globus_result_t                     result,
    void *                              session_arg,
    char *                              username,
    char *                              home_dir)
{
	pthread_mutex_lock(&op->lock);
	{
		// Copy out the response
		op->session_arg = session_arg;
		op->init_result = result;
		// Notify the control logic
		pthread_cond_signal(&op->cond);
	}
	pthread_mutex_unlock(&op->lock);
}

/**********************************************************
 *
 *  DSI stat_func() callbacks
 *
 *********************************************************/
static struct stat_chain *
copy_out_stat_array(
    globus_result_t     result,
    globus_gfs_stat_t * stat_array,
    int                 stat_count)
{
	struct stat_chain * sc = calloc(1, sizeof(*sc));
	sc->result     = result;

	// Why does stat_count = 1 on error?
	if (result == GLOBUS_SUCCESS)
	{
		sc->stat_count = stat_count;
		sc->stat_array = calloc(stat_count, sizeof(*stat_array));

		for (int i = 0; i < stat_count; i++)
		{
			sc->stat_array[i] = stat_array[i];
			sc->stat_array[i].name = safe_strdup(stat_array[i].name);
			sc->stat_array[i].symlink_target = safe_strdup(stat_array[i].symlink_target);
		}
	}

	return sc;
}

void
globus_gridftp_server_finished_stat_partial(
    globus_gfs_operation_t              op,
    globus_result_t                     result,
    globus_gfs_stat_t *                 stat_array,
    int                                 stat_count)
{
	struct stat_chain * sc = NULL;
	sc = copy_out_stat_array(result, stat_array, stat_count);

	pthread_mutex_lock(&op->lock);
	{
		// Link us in
		sc->next = op->stat.chain;
		op->stat.chain = sc;

		// Notify the control logic
		pthread_cond_signal(&op->cond);
	}
	pthread_mutex_unlock(&op->lock);
}

void
globus_gridftp_server_finished_stat(
    globus_gfs_operation_t              op,
    globus_result_t                     result,
    globus_gfs_stat_t *                 stat_array,
    int                                 stat_count)
{
	struct stat_chain * sc = NULL;
	sc = copy_out_stat_array(result, stat_array, stat_count);

	pthread_mutex_lock(&op->lock);
	{
		// Link us in
		sc->next = op->stat.chain;
		op->stat.chain = sc;

		// Indicate completion
		op->stat.finished = 1;

		// Notify the control logic
		pthread_cond_signal(&op->cond);
	}
	pthread_mutex_unlock(&op->lock);
}

/**********************************************************
 *
 *  DSI recv_func() callbacks
 *
 *********************************************************/
void
globus_gridftp_server_get_block_size(
    globus_gfs_operation_t              op,
    globus_size_t *                     block_size)
{
	pthread_mutex_lock(&op->lock);
	{
		*block_size = op->recv.block_size;
	}
	pthread_mutex_unlock(&op->lock);
}

void
globus_gridftp_server_begin_transfer(
    globus_gfs_operation_t              op,
    int                                 event_mask,
    void *                              event_arg)
{
	/* Nothing to do here. */
}

void
globus_gridftp_server_get_write_range(
    globus_gfs_operation_t              op,
    globus_off_t *                      offset,
    globus_off_t *                      length)
{
	pthread_mutex_lock(&op->lock);
	{
		// Unlink the next range
		struct range * r = op->recv.ranges;
		op->recv.ranges = r->next;

		// Copy the range out
		*offset = r->offset;
		*length = r->length;

		// Free this range
		free(r);
	}
	pthread_mutex_unlock(&op->lock);
}

void
globus_gridftp_server_get_optimal_concurrency(
    globus_gfs_operation_t              op,
    int *                               count)
{
	pthread_mutex_lock(&op->lock);
	{
		*count = op->recv.concurrency;
	}
	pthread_mutex_unlock(&op->lock);
}

globus_result_t
globus_gridftp_server_register_read(
    globus_gfs_operation_t              op,
    globus_byte_t *                     buffer,
    globus_size_t                       length,
    globus_gridftp_server_read_cb_t     callback,
    void *                              user_arg)
{
	struct buffer * r = calloc(1, sizeof(*r));

	r->buffer   = buffer;
	r->length   = length;
	r->callback = callback;
	r->user_arg = user_arg;

	pthread_mutex_lock(&op->lock);
	{
		// Link us in
		r->next = op->recv.buffers;
		op->recv.buffers = r;

		// Wake the control logic
		pthread_cond_signal(&op->cond);
	}
	pthread_mutex_unlock(&op->lock);

	return GLOBUS_SUCCESS;
}

/* AKA restart markers. */
void
globus_gridftp_server_update_range_recvd(
    globus_gfs_operation_t              op,
    globus_off_t                        offset,
    globus_off_t                        length)
{
}

/* AKA performance markers. */
void
globus_gridftp_server_update_bytes_recvd(
    globus_gfs_operation_t              op,
    globus_off_t                        length)
{
}

void
globus_gridftp_server_finished_transfer(
    globus_gfs_operation_t              op,
    globus_result_t                     result)
{
	pthread_mutex_lock(&op->lock);
	{
		op->recv.finished = 1;
		op->recv.result   = result;

		// Wake the control logic
		pthread_cond_signal(&op->cond);
	}
	pthread_mutex_unlock(&op->lock);
}

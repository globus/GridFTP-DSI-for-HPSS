#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stage_test.h>

static void
add_to_empty_list(void **state)
{
	struct request * list = NULL;
	bitfile_id_t bitfile_id;
	hpss_reqid_t request_id;

	memset(&bitfile_id, 1, sizeof(bitfile_id_t));
	memset(&request_id, 2, sizeof(hpss_reqid_t));

	list = request_list_add(list, &bitfile_id, request_id);

	assert_non_null(list);
	assert_null(list->next);
	free(list);
}

static void
add_to_nonempty_list(void **state)
{
	struct request * list = calloc(1, sizeof(struct request));

	bitfile_id_t bitfile_id;
	hpss_reqid_t request_id;

	memset(&bitfile_id, 1, sizeof(bitfile_id_t));
	memset(&request_id, 2, sizeof(hpss_reqid_t));

	list = request_list_add(list, &bitfile_id, request_id);

	assert_non_null(list);
	assert_non_null(list->next);
	assert_null(list->next->next);
	free(list->next);
	free(list);
}

static void
del_from_empty_list(void **state)
{
	struct request * list = NULL;

	struct request request;

	list = request_list_del(list, &request);
	assert_null(list);
}

static void
del_from_list_of_1(void **state)
{
	struct request request;
	memset(&request, 1, sizeof(struct request));

	struct request * list = calloc(1, sizeof(struct request));
	memcpy(list, &request, sizeof(struct request));
	list->next = NULL;

	list = request_list_del(list, &request);
	assert_null(list);
}

static void
del_first_from_list(void **state)
{
	struct request * list = calloc(1, sizeof(struct request));
	memset(list, 1, sizeof(struct request));

	list->next = calloc(1, sizeof(struct request));
	memset(list->next, 2, sizeof(struct request));

	list->next->next = calloc(1, sizeof(struct request));
	memset(list->next->next, 3, sizeof(struct request));
	list->next->next->next = NULL;

	struct request request;
	memset(&request, 1, sizeof(struct request));

	list = request_list_del(list, &request);

	assert_non_null(list);
	memset(&request, 2, sizeof(struct request));
	request.next = list->next;
	assert_memory_equal(list, &request, sizeof(struct request));

	assert_non_null(list->next);
	memset(&request, 3, sizeof(struct request));
	request.next = NULL;
	assert_memory_equal(list->next, &request, sizeof(struct request));

	assert_null(list->next->next);

	free(list->next);
	free(list);
}

static void
del_middle_from_list(void **state)
{
	struct request * list = calloc(1, sizeof(struct request));
	memset(list, 1, sizeof(struct request));

	list->next = calloc(1, sizeof(struct request));
	memset(list->next, 2, sizeof(struct request));

	list->next->next = calloc(1, sizeof(struct request));
	memset(list->next->next, 3, sizeof(struct request));
	list->next->next->next = NULL;

	struct request request;
	memset(&request, 2, sizeof(struct request));

	list = request_list_del(list, &request);

	assert_non_null(list);
	memset(&request, 1, sizeof(struct request));
	request.next = list->next;
	assert_memory_equal(list, &request, sizeof(struct request));

	assert_non_null(list->next);
	memset(&request, 3, sizeof(struct request));
	request.next = NULL;
	assert_memory_equal(list->next, &request, sizeof(struct request));

	assert_null(list->next->next);

	free(list->next);
	free(list);
}

static void
del_last_from_list(void **state)
{
	struct request * list = calloc(1, sizeof(struct request));
	memset(list, 1, sizeof(struct request));

	list->next = calloc(1, sizeof(struct request));
	memset(list->next, 2, sizeof(struct request));

	list->next->next = calloc(1, sizeof(struct request));
	memset(list->next->next, 3, sizeof(struct request));
	list->next->next->next = NULL;

	struct request request;
	memset(&request, 3, sizeof(struct request));

	list = request_list_del(list, &request);

	assert_non_null(list);
	memset(&request, 1, sizeof(struct request));
	request.next = list->next;
	assert_memory_equal(list, &request, sizeof(struct request));

	assert_non_null(list->next);
	memset(&request, 2, sizeof(struct request));
	request.next = NULL;
	assert_memory_equal(list->next, &request, sizeof(struct request));

	assert_null(list->next->next);

	free(list->next);
	free(list);
}

static void
find_on_empty_list(void **state)
{
	struct request * list = NULL;
	bitfile_id_t bitfile_id;
	memset(&bitfile_id, 1, sizeof(bitfile_id_t));

	list = request_list_find(list, &bitfile_id);
	assert_null(list);
}

static void
find_in_list(void **state)
{
	struct request * list = calloc(1, sizeof(struct request));
	memset(list, 1, sizeof(struct request));

	list->next = calloc(1, sizeof(struct request));
	memset(list->next, 2, sizeof(struct request));

	list->next->next = calloc(1, sizeof(struct request));
	memset(list->next->next, 3, sizeof(struct request));
	list->next->next->next = NULL;

	struct request * request_ptr;
	struct request   request;

	memset(&request, 1, sizeof(struct request));
	request.next = list->next;
	request_ptr = request_list_find(list, &request.bitfile_id);
	memcpy(request_ptr, &request, sizeof(struct request));

	memset(&request, 2, sizeof(struct request));
	request.next = list->next->next;
	request_ptr = request_list_find(list, &request.bitfile_id);
	memcpy(request_ptr, &request, sizeof(struct request));

	memset(&request, 3, sizeof(struct request));
	request.next = NULL;
	request_ptr = request_list_find(list, &request.bitfile_id);
	memcpy(request_ptr, &request, sizeof(struct request));

	free(list->next->next);
	free(list->next);
	free(list);
}

int
main()
{
	const struct CMUnitTest tests[] = {
		cmocka_unit_test(add_to_empty_list),
		cmocka_unit_test(add_to_nonempty_list),
		cmocka_unit_test(del_from_empty_list),
		cmocka_unit_test(del_from_list_of_1),
		cmocka_unit_test(del_first_from_list),
		cmocka_unit_test(del_middle_from_list),
		cmocka_unit_test(del_last_from_list),
		cmocka_unit_test(find_on_empty_list),
		cmocka_unit_test(find_in_list),
	};

	return cmocka_run_group_tests(tests, NULL, NULL);
}

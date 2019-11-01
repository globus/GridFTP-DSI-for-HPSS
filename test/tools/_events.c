#include <stdlib.h>

#include "_events.h"

struct listener {
    void (*Handler)(const struct test_event *);
    struct listener * Next;
};

static struct listener * EventListeners = NULL;

void
set_event_listener(void (*Handler)(const struct test_event *))
{
    struct listener * listener = calloc(1, sizeof(struct listener));
    listener->Handler = Handler;
    listener->Next = EventListeners;
    EventListeners = listener;
}

void
clear_event_listeners()
{
    struct listener * l = NULL;
    while ((l = EventListeners))
    {
        EventListeners = EventListeners->Next;
	free(l);
    }
}

void
send_event(const struct test_event TE)
{
    struct listener * listener;
    for (listener = EventListeners; listener != NULL; listener = listener->Next)
    {
        listener->Handler(&TE);
    }
}

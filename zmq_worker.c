#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdbool.h>
#include <zmq.h>

#define MAX_MSG_LEN 1500

typedef struct
{
    char **keys;
    int *vals;
    size_t len;
    size_t cap;
} Dictionary;

static char *xstrdup(const char *s)
{
    size_t n = strlen(s);
    char *d = malloc(n + 1);
    if (!d)
    {
        return NULL;
    }
    memcpy(d, s, n + 1);
    return d;
}

static int ensureCap(Dictionary *d, size_t need)
{
    if (need <= d->cap)
        return 0;
    size_t newcap = d->cap ? d->cap * 2 : 8;
    while (newcap < need)
        newcap *= 2;

    char **newKey = realloc(d->keys, newcap * sizeof(*newKey));
    int *newValue = realloc(d->vals, newcap * sizeof(*newValue));
    if (!newKey || !newValue)
    {
        return -1;
    }

    d->keys = newKey;
    d->vals = newValue;
    d->cap = newcap;
    return 0;
}

static int addOrIncrementDictionary(Dictionary *d, const char *word)
{
    for (size_t i = 0; i < d->len; i++)
    {
        if (strcmp(d->keys[i], word) == 0)
        {
            d->vals[i] += 1;
            return 0;
        }
    }
    if (ensureCap(d, d->len + 1) != 0)
        return -1;

    d->keys[d->len] = xstrdup(word);
    if (!d->keys[d->len])
        return -1;
    d->vals[d->len] = 1;
    d->len++;
    return 0;
}

static void freeDictionary(Dictionary *d)
{
    for (size_t i = 0; i < d->len; i++)
        free(d->keys[i]);
    free(d->keys);
    free(d->vals);
}

static void getValuesAsString(Dictionary *d, char *outbuf, size_t outbufsize, bool isMapCall)
{
    size_t offset = 0;
    if (outbufsize == 0)
        return;

    for (size_t i = 0; i < d->len; i++)
    {
        const char *key = d->keys[i];
        int v = d->vals[i];
        if (v < 0)
        {
            v = 0;
        }

        int n = snprintf(outbuf + offset, outbufsize - offset, "%s", key);
        if (n < 0 || (size_t)n >= outbufsize - offset)
        {
            break;
        }
        offset += (size_t)n;

        size_t space_left = outbufsize - offset - 1;
        if (isMapCall)
        {
            size_t ones = (size_t)v;
            if (ones > space_left)
            {
                ones = space_left;
            }

            memset(outbuf + offset, '1', ones);
            offset += ones;
        }
        else
        {
            n = snprintf(outbuf + offset, outbufsize - offset, "%d", v);
            if (n < 0 || (size_t)n >= outbufsize - offset)
            {
                break;
            }
            offset += (size_t)n;
        }
    }

    outbuf[(offset < outbufsize) ? offset : (outbufsize - 1)] = '\0';
}

void cleanString(char *string)
{
    for (; *string; string++)
    {
        unsigned char unsignedChar = (unsigned char)*string;
        if (isalpha(unsignedChar))
            *string = (char)tolower(unsignedChar);
        else
            *string = ' ';
    }
}

char *map(const char *payload)
{
    static char outbuf[MAX_MSG_LEN];
    outbuf[0] = '\0';

    char *payloadCopy = strdup(payload);
    if (!payloadCopy)
        return outbuf;

    cleanString(payloadCopy);

    Dictionary dict = (Dictionary){0};

    int startIndex = 0;
    int endIndex = 0;

    for (char *p = payloadCopy; *p != '\0'; p++, endIndex++)
    {
        if (*p == ' ')
        {
            if (endIndex > startIndex)
            {
                char saved = *p;
                *p = '\0';
                (void)addOrIncrementDictionary(&dict, payloadCopy + startIndex);
                *p = saved;
            }
            startIndex = endIndex + 1;
        }
    }

    if (endIndex > startIndex)
    {
        (void)addOrIncrementDictionary(&dict, payloadCopy + startIndex);
    }

    getValuesAsString(&dict, outbuf, sizeof(outbuf), true);

    freeDictionary(&dict);
    free(payloadCopy);

    return outbuf;
}

char *reduce(const char *payload)
{
    static char outbuf[MAX_MSG_LEN];
    outbuf[0] = '\0';

    Dictionary dict = (Dictionary){0};

    const char *p = payload;

    while (*p != '\0')
    {
        if (!isalpha((unsigned char)*p))
        {
            p++;
            continue;
        }

        const char *wordStart = p;
        while (isalpha((unsigned char)*p))
            p++;
        size_t wlen = (size_t)(p - wordStart);

        char word[256];
        if (wlen >= sizeof(word))
            wlen = sizeof(word) - 1;
        memcpy(word, wordStart, wlen);
        word[wlen] = '\0';

        int count = 0;

        if (*p == '1')
        {
            while (*p == '1')
            {
                count++;
                p++;
            }
        }
        else if (isdigit((unsigned char)*p))
        {
            while (isdigit((unsigned char)*p))
            {
                count = count * 10 + (*p - '0');
                p++;
            }
        }

        for (int k = 0; k < count; k++)
            addOrIncrementDictionary(&dict, word);
    }

    getValuesAsString(&dict, outbuf, sizeof(outbuf), false);

    freeDictionary(&dict);

    return outbuf;
}

int main(int argc, char **argv)
{
    if (argc < 2)
    {
        return EXIT_FAILURE;
    }

    const int workerCount = argc - 1;

    void *zmq = zmq_ctx_new();
    if (!zmq)
    {
        perror("zmq_ctx_new");
        return EXIT_FAILURE;
    }

    void **sockets = calloc((size_t)workerCount, sizeof(void *));
    zmq_pollitem_t *items = calloc((size_t)workerCount, sizeof(zmq_pollitem_t));
    if (!sockets || !items)
    {
        perror("calloc");
        free(sockets);
        free(items);
        zmq_ctx_term(zmq);
        return EXIT_FAILURE;
    }

    for (int i = 0; i < workerCount; i++)
    {
        int port = atoi(argv[i + 1]);
        if (port <= 0 || port > 65535)
        {
            fprintf(stderr, "Invalid port: %s\n", argv[i + 1]);
            zmq_ctx_term(zmq);
            return EXIT_FAILURE;
        }

        sockets[i] = zmq_socket(zmq, ZMQ_REP);
        if (!sockets[i])
        {
            perror("zmq_socket");
            zmq_ctx_term(zmq);
            return EXIT_FAILURE;
        }

        char endpoint[64];
        snprintf(endpoint, sizeof(endpoint), "tcp://*:%d", port);

        if (zmq_bind(sockets[i], endpoint) != 0)
        {
            perror("zmq_bind");
            fprintf(stderr, "Failed to bind to %s\n", endpoint);
            zmq_ctx_term(zmq);
            return EXIT_FAILURE;
        }

        items[i].socket = sockets[i];
        items[i].events = ZMQ_POLLIN;
    }

    int active = workerCount;

    while (active > 0)
    {
        if (zmq_poll(items, workerCount, -1) < 0)
        {
            perror("zmq_poll");
            break;
        }

        for (int i = 0; i < workerCount; i++)
        {
            if (!items[i].socket)
            {
                continue;
            }
            if ((items[i].revents & ZMQ_POLLIN) == 0)
            {
                continue;
            }

            char inbuf[MAX_MSG_LEN];
            memset(inbuf, 0, sizeof(inbuf));

            int n = zmq_recv(items[i].socket, inbuf, (int)sizeof(inbuf) - 1, 0);
            if (n < 0)
            {
                (void)zmq_send(items[i].socket, "", 1, 0);
                continue;
            }
            inbuf[(n >= (int)sizeof(inbuf)) ? (int)sizeof(inbuf) - 1 : n] = '\0';

            if (memcmp(inbuf, "map", 3) == 0)
            {
                const char *payload = inbuf + 3;
                const char *reply = map(payload);
                (void)zmq_send(items[i].socket, reply, (int)strlen(reply) + 1, 0);
                continue;
            }

            if (memcmp(inbuf, "red", 3) == 0)
            {
                const char *payload = inbuf + 3;
                const char *reply = reduce(payload);
                (void)zmq_send(items[i].socket, reply, (int)strlen(reply) + 1, 0);
                continue;
            }

            if (memcmp(inbuf, "rip", 3) == 0)
            {
                (void)zmq_send(items[i].socket, "rip", 4, 0);
                zmq_close(items[i].socket);
                items[i].socket = NULL;
                items[i].events = 0;
                active--;
                continue;
            }

            (void)zmq_send(items[i].socket, "", 1, 0);
        }
    }

    free(sockets);
    free(items);
    zmq_ctx_term(zmq);
    return EXIT_SUCCESS;
}
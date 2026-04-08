#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <errno.h>
#include <stdlib.h>

static char *resourceNames[100];
static char *resources[100];
static int currentResource = 0;

int findResourceByName(char *resourceNames[], char *resourceName, int count)
{
    for (int i = 0; i < count; i++)
    {
        if (strcmp(resourceNames[i], resourceName) == 0)
        {
            return i;
        }
    }
    return -1;
}

int answerPort(int sockfd, const char *request)
{
    const char *reply = NULL;
    char method[16], path[256], version[16];

    // 400 if empty
    if (request == NULL || request[0] == '\0')
    {
        reply = "HTTP/1.1 400 Bad Request\r\n\r\n";
    }
    // parse first line into method / path / version
    else if (sscanf(request, "%15s %255s %15s", method, path, version) != 3)
    {
        reply = "HTTP/1.1 400 Bad Request\r\n\r\n";
    }
    // GET → 200 or 404
    else if (strcmp(method, "GET") == 0)
    {

        if (strncmp(path, "/dynamic/", 9) == 0)
        {
            for (int i = 0; i < currentResource; i++)
            {
                char dynamicResourcePath[512];
                snprintf(dynamicResourcePath, sizeof(dynamicResourcePath), "/dynamic/%s", resourceNames[i]);
                if (strcmp(path, dynamicResourcePath) == 0)
                {
                    char buf[4096];
                    snprintf(buf, sizeof(buf),
                             "HTTP/1.1 200 OK\r\n"
                             "Content-Length: %zu\r\n"
                             "Connection: close\r\n"
                             "\r\n"
                             "%s",
                             strlen(resources[i]), resources[i]);

                    reply = buf;
                    break;
                }
            }
            if (reply == NULL)
            {
                reply = "HTTP/1.1 404 Not Found\r\n"
                        "Content-Length: 0\r\n"
                        "Connection: close\r\n"
                        "\r\n";
            }
        }
        else
        {

            // reply = "HTTP/1.1 404 Not Found\r\n\r\n";
            if (strcmp(path, "/static/foo") == 0)
            {
                reply = "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nFoo";
            }
            else if (strcmp(path, "/static/bar") == 0)
            {
                reply = "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nBar";
            }
            else if (strcmp(path, "/static/baz") == 0)
            {
                reply = "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nBaz";
            }
            else
            {
                reply = "HTTP/1.1 404 Not Found\r\n\r\n";
            }
        }
    }
    else if (strcmp(method, "PUT") == 0)
    {
        if (strncmp(path, "/dynamic/", 9) != 0)
        {
            reply = "HTTP/1.1 403 Forbidden\r\n"
                    "Content-Length: 0\r\n"
                    "Connection: close\r\n"
                    "\r\n";
            return 0;
        }
        char *currentResourceName = strdup(strrchr(path, '/') + 1);
        int indexIfResourceAvailable = findResourceByName(resourceNames, currentResourceName, currentResource);

        const char *body = strstr(request, "\r\n\r\n");
        if (body != NULL)
        {
            body += 4;
        }
        else
        {
            body = "";
        }

        if (indexIfResourceAvailable != -1)
        {
            resources[indexIfResourceAvailable] = strdup(body);
            reply = "HTTP/1.1 204 No Content\r\n"
                    "Content-Length: 0\r\n"
                    "Connection: close\r\n"
                    "\r\n";
        }
        else
        {
            resourceNames[currentResource] = currentResourceName;
            resources[currentResource] = strdup(body);
            currentResource++;
            reply = "HTTP/1.1 201 Created\r\n"
                    "Content-Length: 0\r\n"
                    "Connection: close\r\n"
                    "\r\n";
        }
    }
    else if (strcmp(method, "DELETE") == 0)
    {
        if (strncmp(path, "/dynamic/", 9) != 0)
        {
            reply = "HTTP/1.1 403 Forbidden\r\n"
                    "Content-Length: 0\r\n"
                    "Connection: close\r\n"
                    "\r\n";
            return 0;
        }
        char *currentResourceName = strdup(strrchr(path, '/') + 1);
        int indexIfResourceAvailable = findResourceByName(resourceNames, currentResourceName, currentResource);
        if (indexIfResourceAvailable != -1)
        {
            int last = currentResource - 1;
            resourceNames[indexIfResourceAvailable] = resourceNames[last];
            resources[indexIfResourceAvailable] = resources[last];
            resourceNames[last] = NULL;
            resources[last] = NULL;
            currentResource--;
            reply = "HTTP/1.1 204 No Content\r\n"
                    "Content-Length: 0\r\n"
                    "Connection: close\r\n"
                    "\r\n";
        }
        else
        {
            reply = "HTTP/1.1 404 Not Found\r\n"
                    "Content-Length: 0\r\n"
                    "Connection: close\r\n"
                    "\r\n";
        }
    }
    // HEAD, POST, PUT, DELETE → 501
    else if (strcmp(method, "HEAD") == 0 ||
             strcmp(method, "POST") == 0)
    {
        reply = "HTTP/1.1 501 Not Implemented\r\n\r\n";
    }
    else
    {
        reply = "HTTP/1.1 400 Bad Request\r\n\r\n";
    }

    ssize_t sent = send(sockfd, reply, strlen(reply), 0);
    if (sent < 0)
    {
        if (errno == EPIPE || errno == ECONNRESET)
            return -1;
        perror("send");
        return -1;
    }
    return 0;
}

int receivePort(int sockfd, char *buf, size_t bufsize)
{
    int n = recv(sockfd, buf, bufsize - 1, 0);
    if (n <= 0)
    {
        buf[0] = '\0';
        return (int)n;
    }
    buf[n] = '\0';
    return n;
}

int bindPort(char *port, char *address)
{
    struct sockaddr_storage their_addr;
    socklen_t addr_size;
    struct addrinfo hints, *res;
    int sockfd, new_fd;

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = 0;

    if (getaddrinfo(address, port, &hints, &res) != 0)
    {
        perror("getaddrinfo");
        return 1;
    }

    sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (sockfd < 0)
    {
        perror("socket");
        freeaddrinfo(res);
        return 1;
    }

    int optval = 1;

    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));

    if (bind(sockfd, res->ai_addr, res->ai_addrlen) != 0)
    {
        perror("bind");
        close(sockfd);
        freeaddrinfo(res);
        return 1;
    }

    if (listen(sockfd, 10) != 0)
    {
        perror("listen");
        close(sockfd);
        freeaddrinfo(res);
        return 1;
    }

    freeaddrinfo(res);

    printf("LISTENING on %s:%s\n", address, port);
    fflush(stdout);

    while (1)
    {
        addr_size = sizeof their_addr;
        new_fd = accept(sockfd, (struct sockaddr *)&their_addr, &addr_size);
        if (new_fd < 0)
        {
            perror("accept");
            continue;
        }

        char buf[2048];
        size_t total = 0;
        ssize_t n;

        while ((n = receivePort(new_fd, buf + total, sizeof(buf) - total)) > 0)
        {
            total += (size_t)n;
            buf[total] = '\0';
            while (1)
            {
                char *header_end = strstr(buf, "\r\n\r\n");
                if (header_end == NULL)
                {
                    break;
                }

                size_t header_len = (size_t)(header_end - buf) + 4;

                size_t content_length = 0;
                char *cl = strstr(buf, "Content-Length:");
                if (cl != NULL && cl < header_end)
                {
                    cl += strlen("Content-Length:");
                    while (*cl == ' ')
                        cl++;
                    content_length = (size_t)strtoul(cl, NULL, 10);
                }

                size_t message_len = header_len + content_length;

                if (total < message_len)
                {
                    break;
                }

                printf("Empfangen Paket: %.*s\n", (int)header_len, buf);
                fflush(stdout);

                char saved = buf[message_len];
                buf[message_len] = '\0';

                if (answerPort(new_fd, buf) < 0)
                {
                    buf[message_len] = saved;
                    break;
                }

                buf[message_len] = saved;

                size_t rest = total - message_len;
                memmove(buf, buf + message_len, rest);
                total = rest;
                buf[total] = '\0';
            }
        }

        close(new_fd);
    }

    return 0;
}

int main(int argc, char *argv[])
{
    if (argc != 3)
    {
        fprintf(stderr, "Usage: %s [address] <port>\n", argv[0]);
        return 1;
    }

    bindPort(argv[2], argv[1]);
    return 0;
}

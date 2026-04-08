#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "data.h"
#include "http.h"
#include "util.h"
char selfIp[32];
uint16_t selfPort;

#define MAX_RESOURCES 100

uint16_t predId;
uint16_t succId;
uint16_t myId;
uint16_t succPort;
char succIp[32];

struct tuple resources[MAX_RESOURCES] = {
    {"/static/foo", "Foo", sizeof "Foo" - 1},
    {"/static/bar", "Bar", sizeof "Bar" - 1},
    {"/static/baz", "Baz", sizeof "Baz" - 1}};

// --- Praxis 2: Aufgabe 1.3 ---
int udp_sock = -1;

// Aufgabe 1.6
// Key stellt jeweils den Hash der Ressource dar,
// während Value die Information zu dem Lookup ist
struct lookup_information *lookup_table[10];

void add_to_lookup_table(struct lookup_information *info)
{
    if (lookup_table[9] != NULL)
    {
        free(lookup_table[9]);
    }
    for (int i = 8; i > 0; i--)
    {
        lookup_table[i + 1] = lookup_table[i];
    }
    lookup_table[0] = info;
}

struct lookup_information *get_from_lookup_table(uint16_t hash)
{
    for (int i = 0; i < 10; i++)
    {
        if (lookup_table[i] == NULL)
        {
            continue;
        }
        if (lookup_table[i]->pre_responsible_id < hash && hash <= lookup_table[i]->responsible_id)
        {
            return lookup_table[i];
        }
    }
    return NULL;
}

void send_lookup(uint16_t hash, const char *self_ip, uint16_t self_port);
void send_503(int conn);

void send_503(int conn)
{
    const char *msg =
        "HTTP/1.1 503 Service Unavailable\r\n"
        "Retry-After: 1\r\n"
        "Content-Length: 0\r\n"
        "\r\n";
    send(conn, msg, strlen(msg), 0);
}

void send_lookup(uint16_t hash, const char *self_ip, uint16_t self_port)
{
    uint8_t msg[11];

    msg[0] = 0; // Message Type: Lookup

    uint16_t h = htons(hash);
    memcpy(msg + 1, &h, 2);

    uint16_t id = htons(myId);
    memcpy(msg + 3, &id, 2);

    struct in_addr ip;
    inet_pton(AF_INET, self_ip, &ip);
    memcpy(msg + 5, &ip.s_addr, 4);

    uint16_t p = htons(self_port);
    memcpy(msg + 9, &p, 2);

    struct sockaddr_in dest = {0};
    dest.sin_family = AF_INET;
    dest.sin_port = htons(succPort);
    inet_pton(AF_INET, succIp, &dest.sin_addr);

    sendto(udp_sock, msg, sizeof(msg), 0,
           (struct sockaddr *)&dest, sizeof(dest));
}

/**
 * Sends an HTTP reply to the client based on the received request.
 *
 * @param conn      The file descriptor of the client connection socket.
 * @param request   A pointer to the struct containing the parsed request
 * information.
 */
void send_reply(int conn, struct request *request)
{

    // Create a buffer to hold the HTTP reply
    char buffer[HTTP_MAX_SIZE];
    char *reply = buffer;
    size_t offset = 0;

    fprintf(stderr, "Handling %s request for %s (%lu byte payload)\n",
            request->method, request->uri, request->payload_length);

    if (strcmp(request->method, "GET") == 0)
    {
        // Find the resource with the given URI in the 'resources' array.
        size_t resource_length;
        const char *resource =
            get(request->uri, resources, MAX_RESOURCES, &resource_length);

        uint16_t resourceHash = pseudo_hash((unsigned char *)request->uri, strlen(request->uri));

        // bool responsible = !(myId < resourceHash && resourceHash <= succId);
        bool responsible;
        if (predId < myId)
            responsible = (predId < resourceHash && resourceHash <= myId);
        else
            responsible = (predId < resourceHash || resourceHash <= myId);

        // --- Aufgabe 1.3: leerer Adressraum → Lookup ---
        /*if (succId == (uint16_t)(myId + 1))
        {
            send_lookup(resourceHash, selfIp, selfPort);
            send_503(conn);
            return;
        }*/

        if (responsible)
        {
            if (resource)
            {
                size_t payload_offset =
                    sprintf(reply, "HTTP/1.1 200 OK\r\nContent-Length: %lu\r\n\r\n",
                            resource_length);
                memcpy(reply + payload_offset, resource, resource_length);
                offset = payload_offset + resource_length;
            }
            else
            {
                reply = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
                offset = strlen(reply);
            }
        }
        else
        {
            struct lookup_information *info = get_from_lookup_table(resourceHash);
            if (info == NULL)
            {
                if (!((uint16_t)(predId + 1) == myId && (uint16_t)(myId + 1) == succId))
                {
                    offset = sprintf(
                        reply,
                        "HTTP/1.1 303 See Other\r\n"
                        "Location: http://%s:%u%s\r\n"
                        "Content-Length: 0\r\n\r\n",
                        succIp, succPort, request->uri);
                } else {
                    send_lookup(resourceHash, selfIp, selfPort);
                    send_503(conn);
                    return;
                }
            }
            else
            {
                offset = sprintf(
                    reply,
                    "HTTP/1.1 303 See Other\r\n"
                    "Location: http://%s:%u%s\r\n"
                    "Content-Length: 0\r\n\r\n",
                    info->ip, info->port, request->uri);
            }
        }
    }
    else if (strcmp(request->method, "PUT") == 0)
    {
        // Try to set the requested resource with the given payload in the
        // 'resources' array.
        if (set(request->uri, request->payload, request->payload_length,
                resources, MAX_RESOURCES))
        {
            reply = "HTTP/1.1 204 No Content\r\n\r\n";
        }
        else
        {
            reply = "HTTP/1.1 201 Created\r\nContent-Length: 0\r\n\r\n";
        }
        offset = strlen(reply);
    }
    else if (strcmp(request->method, "DELETE") == 0)
    {
        // Try to delete the requested resource from the 'resources' array
        if (delete(request->uri, resources, MAX_RESOURCES))
        {
            reply = "HTTP/1.1 204 No Content\r\n\r\n";
        }
        else
        {
            reply = "HTTP/1.1 404 Not Found\r\n\r\n";
        }
        offset = strlen(reply);
    }
    else
    {
        reply = "HTTP/1.1 501 Method Not Supported\r\n\r\n";
        offset = strlen(reply);
    }

    // Send the reply back to the client
    if (send(conn, reply, offset, 0) == -1)
    {
        perror("send");
        close(conn);
    }
}

/**
 * Processes an incoming packet from the client.
 *
 * @param conn The socket descriptor representing the connection to the client.
 * @param buffer A pointer to the incoming packet's buffer.
 * @param n The size of the incoming packet.
 *
 * @return Returns the number of bytes processed from the packet.
 *         If the packet is successfully processed and a reply is sent, the
 * return value indicates the number of bytes processed. If the packet is
 * malformed or an error occurs during processing, the return value is -1.
 *
 */
ssize_t process_packet(int conn, char *buffer, size_t n)
{
    struct request request = {
        .method = NULL, .uri = NULL, .payload = NULL, .payload_length = -1};
    ssize_t bytes_processed = parse_request(buffer, n, &request);

    if (bytes_processed > 0)
    {
        send_reply(conn, &request);

        // Check the "Connection" header in the request to determine if the
        // connection should be kept alive or closed.
        const string connection_header = get_header(&request, "Connection");
        if (connection_header && strcmp(connection_header, "close"))
        {
            return -1;
        }
    }
    else if (bytes_processed == -1)
    {
        // If the request is malformed or an error occurs during processing,
        // send a 400 Bad Request response to the client.
        const string bad_request = "HTTP/1.1 400 Bad Request\r\n\r\n";
        send(conn, bad_request, strlen(bad_request), 0);
        printf("Received malformed request, terminating connection.\n");
        close(conn);
        return -1;
    }

    return bytes_processed;
}

/**
 * Sets up the connection state for a new socket connection.
 *
 * @param state A pointer to the connection_state structure to be initialized.
 * @param sock The socket descriptor representing the new connection.
 *
 */
static void connection_setup(struct connection_state *state, int sock)
{
    // Set the socket descriptor for the new connection in the connection_state
    // structure.
    state->sock = sock;

    // Set the 'end' pointer of the state to the beginning of the buffer.
    state->end = state->buffer;

    // Clear the buffer by filling it with zeros to avoid any stale data.
    memset(state->buffer, 0, HTTP_MAX_SIZE);
}

/**
 * Discards the front of a buffer
 *
 * @param buffer A pointer to the buffer to be modified.
 * @param discard The number of bytes to drop from the front of the buffer.
 * @param keep The number of bytes that should be kept after the discarded
 * bytes.
 *
 * @return Returns a pointer to the first unused byte in the buffer after the
 * discard.
 * @example buffer_discard(ABCDEF0000, 4, 2):
 *          ABCDEF0000 ->  EFCDEF0000 -> EF00000000, returns pointer to first 0.
 */
char *buffer_discard(char *buffer, size_t discard, size_t keep)
{
    memmove(buffer, buffer + discard, keep);
    memset(buffer + keep, 0, discard); // invalidate buffer
    return buffer + keep;
}

/**
 * Handles incoming connections and processes data received over the socket.
 *
 * @param state A pointer to the connection_state structure containing the
 * connection state.
 * @return Returns true if the connection and data processing were successful,
 * false otherwise. If an error occurs while receiving data from the socket, the
 * function exits the program.
 */
bool handle_connection(struct connection_state *state)
{
    // Calculate the pointer to the end of the buffer to avoid buffer overflow
    const char *buffer_end = state->buffer + HTTP_MAX_SIZE;

    // Check if an error occurred while receiving data from the socket
    ssize_t bytes_read =
        recv(state->sock, state->end, buffer_end - state->end, 0);
    if (bytes_read == -1)
    {
        perror("recv");
        close(state->sock);
        exit(EXIT_FAILURE);
    }
    else if (bytes_read == 0)
    {
        return false;
    }

    char *window_start = state->buffer;
    char *window_end = state->end + bytes_read;

    ssize_t bytes_processed = 0;
    while ((bytes_processed = process_packet(state->sock, window_start,
                                             window_end - window_start)) > 0)
    {
        window_start += bytes_processed;
    }
    if (bytes_processed == -1)
    {
        return false;
    }

    state->end = buffer_discard(state->buffer, window_start - state->buffer,
                                window_end - window_start);
    return true;
}

/**
 * Derives a sockaddr_in structure from the provided host and port information.
 *
 * @param host The host (IP address or hostname) to be resolved into a network
 * address.
 * @param port The port number to be converted into network byte order.
 *
 * @return A sockaddr_in structure representing the network address derived from
 * the host and port.
 */
static struct sockaddr_in derive_sockaddr(const char *host, const char *port)
{
    struct addrinfo hints = {
        .ai_family = AF_INET,
    };
    struct addrinfo *result_info;

    // Resolve the host (IP address or hostname) into a list of possible
    // addresses.
    int returncode = getaddrinfo(host, port, &hints, &result_info);
    if (returncode)
    {
        fprintf(stderr, "Error parsing host/port");
        exit(EXIT_FAILURE);
    }

    // Copy the sockaddr_in structure from the first address in the list
    struct sockaddr_in result = *((struct sockaddr_in *)result_info->ai_addr);

    // Free the allocated memory for the result_info
    freeaddrinfo(result_info);
    return result;
}

/**
 * Sets up a TCP server socket and binds it to the provided sockaddr_in address.
 *
 * @param addr The sockaddr_in structure representing the IP address and port of
 * the server.
 *
 * @return The file descriptor of the created TCP server socket.
 */
static int setup_server_socket(struct sockaddr_in addr, bool udp)
{
    const int enable = 1;
    const int backlog = 1;

    // Create a socket
    int sock = socket(AF_INET, udp ? SOCK_DGRAM : SOCK_STREAM, 0);
    if (sock == -1)
    {
        perror("socket");
        exit(EXIT_FAILURE);
    }

    // Avoid dead lock on connections that are dropped after poll returns but
    // before accept is called
    if (fcntl(sock, F_SETFL, O_NONBLOCK) == -1)
    {
        perror("fcntl");
        exit(EXIT_FAILURE);
    }

    // Set the SO_REUSEADDR socket option to allow reuse of local addresses
    if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable)) ==
        -1)
    {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }

    // Bind socket to the provided address
    if (bind(sock, (struct sockaddr *)&addr, sizeof(addr)) == -1)
    {
        perror("bind");
        close(sock);
        exit(EXIT_FAILURE);
    }

    if (!udp)
    {
        // Start listening on the socket with maximum backlog of 1 pending
        // connection
        if (listen(sock, backlog))
        {
            perror("listen");
            exit(EXIT_FAILURE);
        }
    }

    return sock;
}

/**
 *  The program expects 3; otherwise, it returns EXIT_FAILURE.
 *
 *  Call as:
 *
 *  ./build/webserver self.ip self.port
 */
int main(int argc, char **argv)
{

    if (argc < 3 || argc > 4)
        return EXIT_FAILURE;

    strncpy(selfIp, argv[1], sizeof(selfIp) - 1);
    selfIp[sizeof(selfIp) - 1] = '\0';
    selfPort = (uint16_t)atoi(argv[2]);

    myId = (argc == 4) ? (uint16_t)atoi(argv[3]) : 0;

    const char *pred_id_env = getenv("PRED_ID");
    const char *succ_id_env = getenv("SUCC_ID");
    const char *succ_ip_env = getenv("SUCC_IP");
    const char *succ_port_env = getenv("SUCC_PORT");

    predId = pred_id_env ? (uint16_t)atoi(pred_id_env) : 0;
    succId = succ_id_env ? (uint16_t)atoi(succ_id_env) : 0;
    succPort = succ_port_env ? (uint16_t)atoi(succ_port_env) : 0;

    if (succ_ip_env)
    {
        strncpy(succIp, succ_ip_env, sizeof(succIp) - 1);
        succIp[sizeof(succIp) - 1] = '\0';
    }
    else
    {
        strncpy(succIp, selfIp, sizeof(succIp) - 1);
        succIp[sizeof(succIp) - 1] = '\0';
    }

    memset(lookup_table, 0, sizeof(lookup_table));

    struct sockaddr_in addr = derive_sockaddr(argv[1], argv[2]);

    // Set up a server socket.
    int server_socket = setup_server_socket(addr, false);
    udp_sock = setup_server_socket(addr, true);

    // Create an array of pollfd structures to monitor sockets.
    struct pollfd sockets[3] = {
        {.fd = server_socket, .events = POLLIN},
        {.fd = udp_sock, .events = POLLIN},
        {.fd = -1, .events = 0},
    };

    struct connection_state state = {0};
    while (true)
    {

        // Use poll() to wait for events on the monitored sockets.
        int ready = poll(sockets, sizeof(sockets) / sizeof(sockets[0]), -1);
        if (ready == -1)
        {
            perror("poll");
            exit(EXIT_FAILURE);
        }

        // Process events on the monitored sockets.
        for (size_t i = 0; i < sizeof(sockets) / sizeof(sockets[0]); i += 1)
        {
            if (sockets[i].revents != POLLIN)
            {
                // If there are no POLLIN events on the socket, continue to the
                // next iteration.
                continue;
            }
            int s = sockets[i].fd;

            if (s == server_socket)
            {
                // If the event is on the server socket, accept a new connection
                // from a client.
                int connection = accept(server_socket, NULL, NULL);
                if (connection == -1 && errno != EAGAIN &&
                    errno != EWOULDBLOCK)
                {
                    close(server_socket);
                    perror("accept");
                    exit(EXIT_FAILURE);
                }
                else
                {
                    connection_setup(&state, connection);

                    // limit to one connection at a time
                    sockets[0].events = 0;
                    sockets[2].fd = connection;
                    sockets[2].events = POLLIN;
                }
            }
            else if (s == udp_sock)
            {
                // ===============================
                // Praxis 2 – Aufgabe 1.4
                // Empfang und Beantwortung von Lookups
                // ===============================

                uint8_t buf[32];
                struct sockaddr_in src;
                socklen_t srclen = sizeof(src);

                // Receive DHT message via UDP
                ssize_t n = recvfrom(udp_sock, buf, sizeof(buf), 0,
                                     (struct sockaddr *)&src, &srclen);
                if (n < 11)
                    continue;

                // Handle LOOKUP messages
                if (buf[0] == 0)
                {
                    // continue;

                    // Extract hash ID
                    uint16_t hash;
                    memcpy(&hash, buf + 1, 2);
                    hash = ntohs(hash);

                    // Prüfen, ob wir selbst oder unser Nachfolger verantwortlich sind
                    bool i_am_responsible = (predId < hash && hash <= myId);
                    bool succ_is_responsible = (myId < hash && hash <= succId);

                    // Falls bekannt, wer verantwortlich ist → Reply senden
                    if (i_am_responsible || succ_is_responsible)
                    {
                        uint8_t reply[11];
                        reply[0] = 1; // Message Type: Reply

                        // Hash ID: ID des Vorgängers der verantwortlichen Node
                        uint16_t resp_pred = htons(i_am_responsible ? predId : myId);
                        memcpy(reply + 1, &resp_pred, 2);

                        // Node ID der verantwortlichen Node
                        uint16_t resp_id = htons(i_am_responsible ? myId : succId);
                        memcpy(reply + 3, &resp_id, 2);

                        // IP der verantwortlichen Node
                        struct in_addr ip;
                        inet_pton(AF_INET,
                                  i_am_responsible ? selfIp : succIp,
                                  &ip);
                        memcpy(reply + 5, &ip.s_addr, 4);

                        // Port der verantwortlichen Node
                        uint16_t resp_port = htons(i_am_responsible ? selfPort : succPort);
                        memcpy(reply + 9, &resp_port, 2);

                        sendto(udp_sock, reply, sizeof(reply), 0,
                               (struct sockaddr *)&src, srclen); // Sicher, dass die Nachricht an src gesendet werden muss?
                        // Kann ja auch sein, dass die Anfrage schon einige Male weitergeleitet wurde
                    }
                    else
                    {
                        // ===============================
                        // Praxis 2 – Aufgabe 1.5
                        // Weiterleiten eines Lookups
                        // ===============================

                        struct sockaddr_in dest = {0};
                        dest.sin_family = AF_INET;
                        dest.sin_port = htons(succPort);
                        inet_pton(AF_INET, succIp, &dest.sin_addr);

                        // Lookup unverändert an Nachfolger weiterleiten
                        sendto(udp_sock, buf, n, 0,
                               (struct sockaddr *)&dest, sizeof(dest));
                    }
                    // Handle REPLY-Messages
                }
                else if (buf[0] == 1)
                {
                    struct lookup_information *info = malloc(sizeof(struct lookup_information));

                    // Extract predecessor ID
                    uint16_t pred_id;
                    memcpy(&pred_id, buf + 1, 2);
                    info->pre_responsible_id = ntohs(pred_id);

                    // Extract responsible ID
                    uint16_t resp_id;
                    memcpy(&resp_id, buf + 3, 2);
                    info->responsible_id = ntohs(resp_id);

                    // Extract IP
                    struct in_addr ip;
                    memcpy(&ip.s_addr, buf + 5, 4);
                    char ip_str[INET_ADDRSTRLEN];
                    inet_ntop(AF_INET, &ip, ip_str, INET_ADDRSTRLEN);
                    info->ip = strdup(ip_str);

                    // Extract Port
                    uint16_t port;
                    memcpy(&port, buf + 9, 2);
                    info->port = ntohs(port);

                    add_to_lookup_table(info);
                }
            }
            else
            {
                assert(s == state.sock);

                // Call the 'handle_connection' function to process the incoming
                // data on the socket.
                bool cont = handle_connection(&state);
                if (!cont)
                { // get ready for a new connection
                    sockets[0].events = POLLIN;
                    sockets[2].fd = -1;
                    sockets[2].events = 0;
                }
            }
        }
    }

    return EXIT_SUCCESS;
}

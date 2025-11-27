#include <sys/socket.h>

#include "dw_poll.h"
#include "dw_debug.h"

// io_uring TODO changed
#ifdef USE_IO_URING
#include <liburing.h>
#endif

// Define BUF_SIZE if not already defined (should match connection.h)
#ifndef BUF_SIZE
#define BUF_SIZE 65536
#endif


extern int use_wait_spinning;

// return value useful to return failure if we allocate memory here in the future
int dw_poll_init(dw_poll_t *p_poll, dw_poll_type_t type) {
    p_poll->poll_type = type;
    switch (p_poll->poll_type) {
    case DW_SELECT:
        p_poll->u.select_fds.n_rd_fd = 0;
        p_poll->u.select_fds.n_wr_fd = 0;
        break;
    case DW_POLL:
        p_poll->u.poll_fds.n_pollfds = 0;
        break;
    case DW_EPOLL:
        sys_check(p_poll->u.epoll_fds.epollfd = epoll_create1(0));
        break;
    #ifdef USE_IO_URING 
    // io_uring TODO changed
        case DW_IO_URING:
            memset(&p_poll->u.io_uring_fds, 0, sizeof(p_poll->u.io_uring_fds));
            int ret = io_uring_queue_init(IO_URING_ENTRIES, &p_poll->u.io_uring_fds.ring, IORING_SETUP_SQPOLL);
            if (ret < 0) {
                errno = -ret;
                return -1;
            }
            // Pre-allocate buffers for async reads
            for (int i = 0; i < IO_URING_ENTRIES; i++) {
                p_poll->u.io_uring_fds.op_tracking[i].buffer = malloc(BUF_SIZE);
            }
            break;
    #endif

    default:
        check(0, "Wrong dw_poll_type");
    }
    return 0;
}

void dw_select_del_rd_pos(dw_poll_t *p_poll, int i) {
    p_poll->u.select_fds.n_rd_fd--;
    if (i < p_poll->u.select_fds.n_rd_fd) {
        p_poll->u.select_fds.rd_fd[i] =
            p_poll->u.select_fds.rd_fd[p_poll->u.select_fds.n_rd_fd];
        p_poll->u.select_fds.rd_flags[i] =
                p_poll->u.select_fds.rd_flags[p_poll->u.select_fds.n_rd_fd];
        p_poll->u.select_fds.rd_aux[i] =
            p_poll->u.select_fds.rd_aux[p_poll->u.select_fds.n_rd_fd];
    }
}

void dw_select_del_wr_pos(dw_poll_t *p_poll, int i) {
    p_poll->u.select_fds.n_wr_fd--;
    if (i < p_poll->u.select_fds.n_wr_fd) {
        p_poll->u.select_fds.wr_fd[i] =
            p_poll->u.select_fds.wr_fd[p_poll->u.select_fds.n_wr_fd];
        p_poll->u.select_fds.wr_flags[i] =
            p_poll->u.select_fds.wr_flags[p_poll->u.select_fds.n_wr_fd];
        p_poll->u.select_fds.wr_aux[i] =
            p_poll->u.select_fds.wr_aux[p_poll->u.select_fds.n_wr_fd];
    }
}

void dw_poll_del_pos(dw_poll_t *p_poll, int i) {
    p_poll->u.poll_fds.n_pollfds--;
    if (i < p_poll->u.poll_fds.n_pollfds) {
        p_poll->u.poll_fds.pollfds[i] = p_poll->u.poll_fds.pollfds[p_poll->u.poll_fds.n_pollfds];
        p_poll->u.poll_fds.flags[i] = p_poll->u.poll_fds.flags[p_poll->u.poll_fds.n_pollfds];
        p_poll->u.poll_fds.aux[i] = p_poll->u.poll_fds.aux[p_poll->u.poll_fds.n_pollfds];
    }
}

void dw_select_add_rd(dw_poll_t *p_poll, int fd, dw_poll_flags flags, uint64_t aux) {
    p_poll->u.select_fds.rd_fd[p_poll->u.select_fds.n_rd_fd] = fd;
    p_poll->u.select_fds.rd_aux[p_poll->u.select_fds.n_rd_fd] = aux;
    p_poll->u.select_fds.rd_flags[p_poll->u.select_fds.n_rd_fd] = flags;
    p_poll->u.select_fds.n_rd_fd++;
}

void dw_select_add_wr(dw_poll_t *p_poll, int fd, dw_poll_flags flags, uint64_t aux) {
    p_poll->u.select_fds.wr_fd[p_poll->u.select_fds.n_wr_fd] = fd;
    p_poll->u.select_fds.wr_aux[p_poll->u.select_fds.n_wr_fd] = aux;
    p_poll->u.select_fds.wr_flags[p_poll->u.select_fds.n_wr_fd] = flags;
    p_poll->u.select_fds.n_wr_fd++;
}

int dw_poll_add(dw_poll_t *p_poll, int fd, dw_poll_flags flags, uint64_t aux) {
    dw_log("dw_poll_add(): fd=%d, p_poll=%p, flags=%08x, aux=%lu\n", fd, p_poll, flags, aux);
    int rv = 0;
    switch (p_poll->poll_type) {
    case DW_SELECT:
        if ((flags & DW_POLLIN && p_poll->u.select_fds.n_rd_fd == MAX_POLLFD)
            || (flags & DW_POLLOUT && p_poll->u.select_fds.n_wr_fd == MAX_POLLFD)) {
            dw_log("Exhausted number of possible fds in select()\n");
            return -1;
        }
        if (flags & DW_POLLIN)
            dw_select_add_rd(p_poll, fd, flags, aux);
        if (flags & DW_POLLOUT)
            dw_select_add_wr(p_poll, fd, flags, aux);
        break;
    case DW_POLL:
        if (p_poll->u.poll_fds.n_pollfds == MAX_POLLFD) {
            dw_log("Exhausted number of possible fds in poll()\n");
            return -1;
        }
        struct pollfd *pev = &p_poll->u.poll_fds.pollfds[p_poll->u.poll_fds.n_pollfds];
        p_poll->u.poll_fds.flags[p_poll->u.poll_fds.n_pollfds] = flags;
        p_poll->u.poll_fds.aux[p_poll->u.poll_fds.n_pollfds] = aux;
        p_poll->u.poll_fds.n_pollfds++;
        pev->fd = fd;
        pev->events = (flags & DW_POLLIN ? POLLIN : 0)
                      | (flags & DW_POLLOUT ? POLLOUT : 0);
        break;
    case DW_EPOLL: {
        struct epoll_event ev = (struct epoll_event) {
            .data.u64 = aux,
            .events = (flags & DW_POLLIN ? EPOLLIN : 0)
                      | (flags & DW_POLLOUT ? EPOLLOUT : 0)
                      | (flags & DW_POLLONESHOT ? EPOLLONESHOT : 0),
        };

        if ((rv = epoll_ctl(p_poll->u.epoll_fds.epollfd, EPOLL_CTL_ADD, fd, &ev)) < 0)
            perror("epoll_ctl() failed: ");
        break; }

    #ifdef USE_IO_URING
        // io_uring TODO changed
        case DW_IO_URING: {
            struct io_uring *ring = &p_poll->u.io_uring_fds.ring;
            struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
            if (!sqe) {
                dw_log("No available SQEs in io_uring\n");
                return -1;
            }
            
            unsigned idx = sqe - ring->sq.sqes;
            
            // Store operation context
            p_poll->u.io_uring_fds.op_tracking[idx].fd = fd;
            p_poll->u.io_uring_fds.op_tracking[idx].aux = aux;
            p_poll->u.io_uring_fds.op_tracking[idx].flags = flags;
            p_poll->u.io_uring_fds.op_tracking[idx].conn_id = -1;
            
            // For listening sockets, use async accept
            if (flags & DW_POLLIN && fd >= 0) {
                // Check if this is a listening socket by checking if it's in listen_socks
                // This is a simplification - you'd need to track listening sockets properly
                struct sockaddr_in client_addr;
                socklen_t client_len = sizeof(client_addr);
                
                struct io_uring_sqe *accept_sqe = io_uring_get_sqe(ring);
                if (accept_sqe) {
                    io_uring_prep_accept(accept_sqe, fd, (struct sockaddr*)&client_addr, 
                                       &client_len, 0);
                    accept_sqe->user_data = idx | (1ULL << 63); // Mark as accept operation
                    p_poll->u.io_uring_fds.op_tracking[idx].op_type = OP_ACCEPT;
                    p_poll->u.io_uring_fds.pending_accepts++;
                }
            } else {
                // For regular sockets, submit async read
                if (flags & DW_POLLIN) {
                    io_uring_prep_read(sqe, fd, p_poll->u.io_uring_fds.op_tracking[idx].buffer, 
                                     BUF_SIZE, 0);
                    p_poll->u.io_uring_fds.op_tracking[idx].op_type = OP_READ;
                    p_poll->u.io_uring_fds.op_tracking[idx].size = BUF_SIZE;
                } else if (flags & DW_POLLOUT) {
                    // For write readiness, we use poll as fallback
                    unsigned poll_events = (flags & DW_POLLIN ? POLLIN : 0) | (flags & DW_POLLOUT ? POLLOUT : 0);
                    io_uring_prep_poll_add(sqe, fd, poll_events);
                    p_poll->u.io_uring_fds.op_tracking[idx].op_type = OP_POLL;
                }
                
                sqe->user_data = idx;
            }
            
            p_poll->u.io_uring_fds.n_outstanding++;

            // Submit batch if we have enough operations
            if (p_poll->u.io_uring_fds.n_outstanding >= IO_URING_BATCH_SIZE) {
                io_uring_submit(ring);
            }

            rv = 0;
            break; }
    #endif

    default:
        check(0, "Wrong dw_poll_type");
    }
    return rv;
}



/* This tolerates on purpose being called after a ONESHOT event, for which
 * poll and select will have deleted the fd in their user-space lists, whilst
 * epoll has it still registered, but with a clear events interest list
 */
int dw_poll_mod(dw_poll_t *p_poll, int fd, dw_poll_flags flags, uint64_t aux) {
    dw_log("dw_poll_mod(): fd=%d, p_poll=%p, flags=%08x, aux=%lu\n", fd, p_poll, flags, aux);
    int rv = 0;
    switch (p_poll->poll_type) {
    case DW_SELECT: {
        int i;
        for (i = 0; i < p_poll->u.select_fds.n_rd_fd; i++)
            if (p_poll->u.select_fds.rd_fd[i] == fd)
                break;
        if (i < p_poll->u.select_fds.n_rd_fd && !(flags & DW_POLLIN))
            dw_select_del_rd_pos(p_poll, i);
        else if (i == p_poll->u.select_fds.n_rd_fd && (flags & DW_POLLIN))
            dw_select_add_rd(p_poll, fd, flags, aux);

        for (i = 0; i < p_poll->u.select_fds.n_wr_fd; i++)
            if (p_poll->u.select_fds.wr_fd[i] == fd)
                break;
        if (i < p_poll->u.select_fds.n_wr_fd && !(flags & DW_POLLOUT))
            dw_select_del_wr_pos(p_poll, i);
        else if (i == p_poll->u.select_fds.n_wr_fd && (flags & DW_POLLOUT))
            dw_select_add_wr(p_poll, fd, flags, aux);
        break; }
    case DW_POLL: {
        int i;
        for (i = 0; i < p_poll->u.poll_fds.n_pollfds; i++) {
            struct pollfd *pev = &p_poll->u.poll_fds.pollfds[i];
            if (pev->fd == fd) {
                pev->events = (flags & DW_POLLIN ? POLLIN : 0) | (flags & DW_POLLOUT ? POLLOUT : 0);
                dw_log("dw_poll_mod(): pev->events=%04x\n", pev->events);
                if (pev->events == 0)
                    dw_poll_del_pos(p_poll, i);
                break;
            }
        }
        if (i == p_poll->u.poll_fds.n_pollfds && flags != 0)
            rv = dw_poll_add(p_poll, fd, flags, aux);
        break; }
    case DW_EPOLL: {
        struct epoll_event ev = {
            .data.u64 = aux,
            .events = (flags & DW_POLLIN ? EPOLLIN : 0) | (flags & DW_POLLOUT ? EPOLLOUT : 0),
        };
        if (flags & (DW_POLLIN | DW_POLLOUT))
            rv = epoll_ctl(p_poll->u.epoll_fds.epollfd, EPOLL_CTL_MOD, fd, &ev);
        else
            rv = epoll_ctl(p_poll->u.epoll_fds.epollfd, EPOLL_CTL_DEL, fd, NULL);
        break; }
        #ifdef USE_IO_URING
            // io_uring TODO changed
            case DW_IO_URING: {
                struct io_uring *ring = &p_poll->u.io_uring_fds.ring;

                // First, remove any existing entries for this fd
                for (unsigned i = 0; i < IO_URING_ENTRIES; i++) {
                    if (p_poll->u.io_uring_fds.op_tracking[i].fd == fd) {
                        // Mark as removed
                        p_poll->u.io_uring_fds.op_tracking[i].fd = -1;
                        p_poll->u.io_uring_fds.n_outstanding--;
                    }
                }

                // If new flags are non-zero, add a new entry
                if (flags != 0) {
                    // Use dw_poll_add to add the modified entry
                    rv = dw_poll_add(p_poll, fd, flags, aux);
                } else {
                    // Just submit any pending changes
                    if (p_poll->u.io_uring_fds.n_outstanding > 0) {
                        int submitted = io_uring_submit(ring);
                        if (submitted < 0)
                            return -1;
                    }
                }
                break; }
        #endif
        
    default:
        check(0, "Wrong dw_poll_type");
    }
    return rv;
}



// return the number of file descriptors expected to iterate with dw_poll_next(),
// or -1 setting errno
int dw_poll_wait(dw_poll_t *p_poll) {
    int rv;
    switch (p_poll->poll_type) {
    case DW_SELECT:
        FD_ZERO(&p_poll->u.select_fds.rd_fds);
        FD_ZERO(&p_poll->u.select_fds.wr_fds);
        FD_ZERO(&p_poll->u.select_fds.ex_fds);
        int max_fd = 0;
        for (int i = 0; i < p_poll->u.select_fds.n_rd_fd; i++) {
            FD_SET(p_poll->u.select_fds.rd_fd[i], &p_poll->u.select_fds.rd_fds);
            if (p_poll->u.select_fds.rd_fd[i] > max_fd)
                max_fd = p_poll->u.select_fds.rd_fd[i];
        }
        for (int i = 0; i < p_poll->u.select_fds.n_wr_fd; i++) {
            FD_SET(p_poll->u.select_fds.wr_fd[i], &p_poll->u.select_fds.wr_fds);
            if (p_poll->u.select_fds.wr_fd[i] > max_fd)
                max_fd = p_poll->u.select_fds.wr_fd[i];
        }
        dw_log("select()ing: max_fd=%d\n", max_fd);
        struct timeval null_tout = { .tv_sec = 0, .tv_usec = 0 };
        rv = select(max_fd + 1, &p_poll->u.select_fds.rd_fds, &p_poll->u.select_fds.wr_fds, &p_poll->u.select_fds.ex_fds, use_wait_spinning ? &null_tout : NULL);
        // make sure we don't wastefully iterate if select() returned 0 fds ready or error
        p_poll->u.select_fds.iter = rv > 0 ? 0 : p_poll->u.select_fds.n_rd_fd + p_poll->u.select_fds.n_wr_fd;
        break;
    case DW_POLL:
        dw_log("poll()ing: n_pollfds=%d\n", p_poll->u.poll_fds.n_pollfds);
        rv = poll(p_poll->u.poll_fds.pollfds, p_poll->u.poll_fds.n_pollfds,
                  use_wait_spinning ? 0 : -1);
        // make sure we don't wastefully iterate if poll() returned 0 fds ready or error
        p_poll->u.poll_fds.iter = rv > 0 ? 0 : p_poll->u.poll_fds.n_pollfds;
        break;
    case DW_EPOLL:
        dw_log("epoll_wait()ing: epollfd=%d\n", p_poll->u.epoll_fds.epollfd);
        rv = epoll_wait(p_poll->u.epoll_fds.epollfd, p_poll->u.epoll_fds.events, MAX_POLLFD,
                        use_wait_spinning ? 0 : -1);
        p_poll->u.epoll_fds.iter = 0;
        if (rv >= 0)
            p_poll->u.epoll_fds.n_events = rv;
        break;
    #ifdef USE_IO_URING
        // io_uring TODO changed
        case DW_IO_URING: {
            struct io_uring *ring = &p_poll->u.io_uring_fds.ring;
            
            // Submit any pending operations
            if (p_poll->u.io_uring_fds.n_outstanding > 0) {
                io_uring_submit(ring);
            }
            
            // Wait for completions - get multiple CQEs in batch
            rv = io_uring_peek_batch_cqe(ring, p_poll->u.io_uring_fds.cqes, MAX_POLL_EVENTS);
            if (rv >= 0) {
                p_poll->u.io_uring_fds.cqe_count = rv;
                p_poll->u.io_uring_fds.cqe_iter = 0;
            }
            break; }
    #endif

    default:
        check(0, "Wrong dw_poll_type");
    }
    return rv;
}



// returned fd is automatically removed from dw_poll if marked 1SHOT
int dw_poll_next(dw_poll_t *p_poll, dw_poll_flags *flags, uint64_t *aux) {
    switch (p_poll->poll_type) {
    case DW_SELECT:
        while (p_poll->u.select_fds.iter < p_poll->u.select_fds.n_rd_fd && !FD_ISSET(p_poll->u.select_fds.rd_fd[p_poll->u.select_fds.iter], &p_poll->u.select_fds.rd_fds))
            p_poll->u.select_fds.iter++;
        if (p_poll->u.select_fds.iter < p_poll->u.select_fds.n_rd_fd && FD_ISSET(p_poll->u.select_fds.rd_fd[p_poll->u.select_fds.iter], &p_poll->u.select_fds.rd_fds)) {
            *aux = p_poll->u.select_fds.rd_aux[p_poll->u.select_fds.iter];
            *flags = DW_POLLIN;

            if (p_poll->u.select_fds.rd_flags[p_poll->u.select_fds.iter] & DW_POLLONESHOT)
                // item iter replaced with last, so we need to check iter again
                dw_select_del_rd_pos(p_poll, p_poll->u.select_fds.iter);
            else
                p_poll->u.select_fds.iter++;
            return 1;
        }
        int n_rd = p_poll->u.select_fds.n_rd_fd;
        while (p_poll->u.select_fds.iter < n_rd + p_poll->u.select_fds.n_wr_fd && !FD_ISSET(p_poll->u.select_fds.wr_fd[p_poll->u.select_fds.iter - n_rd], &p_poll->u.select_fds.wr_fds))
            p_poll->u.select_fds.iter++;
        if (p_poll->u.select_fds.iter < n_rd + p_poll->u.select_fds.n_wr_fd && FD_ISSET(p_poll->u.select_fds.wr_fd[p_poll->u.select_fds.iter - n_rd], &p_poll->u.select_fds.wr_fds)) {
            *aux = p_poll->u.select_fds.wr_aux[p_poll->u.select_fds.iter - n_rd];
            *flags = DW_POLLOUT;
            if (p_poll->u.select_fds.wr_flags[p_poll->u.select_fds.iter - n_rd] & DW_POLLONESHOT)
                // item iter replaced with last, so we need to check iter again
                dw_select_del_wr_pos(p_poll, p_poll->u.select_fds.iter - n_rd);
            else
                p_poll->u.select_fds.iter++;
            return 1;
        }
        break;
    case DW_POLL:
        while (p_poll->u.poll_fds.iter < p_poll->u.poll_fds.n_pollfds && p_poll->u.poll_fds.pollfds[p_poll->u.poll_fds.iter].revents == 0)
            p_poll->u.poll_fds.iter++;
        if (p_poll->u.poll_fds.iter < p_poll->u.poll_fds.n_pollfds && p_poll->u.poll_fds.pollfds[p_poll->u.poll_fds.iter].revents != 0) {
            *flags = 0;
            *flags |= (p_poll->u.poll_fds.pollfds[p_poll->u.poll_fds.iter].revents & POLLIN) ? DW_POLLIN : 0;
            *flags |= (p_poll->u.poll_fds.pollfds[p_poll->u.poll_fds.iter].revents & POLLOUT) ? DW_POLLOUT : 0;
            *flags |= (p_poll->u.poll_fds.pollfds[p_poll->u.poll_fds.iter].revents & POLLERR) ? DW_POLLERR : 0;
            *flags |= (p_poll->u.poll_fds.pollfds[p_poll->u.poll_fds.iter].revents & POLLHUP) ? DW_POLLHUP : 0;
            *aux = p_poll->u.poll_fds.aux[p_poll->u.poll_fds.iter];
            if (p_poll->u.poll_fds.flags[p_poll->u.poll_fds.iter] & DW_POLLONESHOT)
                // item i replaced with last, so we need to check i again
                dw_poll_del_pos(p_poll, p_poll->u.poll_fds.iter);
            else
                p_poll->u.poll_fds.iter++;
            return 1;
        }
        break;
    case DW_EPOLL:
        if (p_poll->u.epoll_fds.iter < p_poll->u.epoll_fds.n_events && p_poll->u.epoll_fds.events[p_poll->u.epoll_fds.iter].events != 0) {
            *flags = 0;
            *flags |= (p_poll->u.epoll_fds.events[p_poll->u.epoll_fds.iter].events & EPOLLIN) ? DW_POLLIN : 0;
            *flags |= (p_poll->u.epoll_fds.events[p_poll->u.epoll_fds.iter].events & EPOLLOUT) ? DW_POLLOUT : 0;
            *flags |= (p_poll->u.epoll_fds.events[p_poll->u.epoll_fds.iter].events & EPOLLERR) ? DW_POLLERR : 0;
            *flags |= (p_poll->u.epoll_fds.events[p_poll->u.epoll_fds.iter].events & EPOLLHUP) ? DW_POLLHUP : 0;
            *aux = p_poll->u.epoll_fds.events[p_poll->u.epoll_fds.iter].data.u64;
            p_poll->u.epoll_fds.iter++;
            return 1;
        }
        break;
    #ifdef USE_IO_URING
        // io_uring TODO changed
        case DW_IO_URING: {
            //if (p_poll->u.io_uring_fds.cqe_iter < p_poll->u.io_uring_fds.cqe_count) {
            while (p_poll->u.io_uring_fds.cqe_iter < p_poll->u.io_uring_fds.cqe_count) {
                struct io_uring *ring = &p_poll->u.io_uring_fds.ring;
                struct io_uring_cqe *cqe = p_poll->u.io_uring_fds.cqes[p_poll->u.io_uring_fds.cqe_iter];
                
                unsigned idx = cqe->user_data & ~(1ULL << 63);
                int is_accept = (cqe->user_data & (1ULL << 63)) != 0;
                
                struct io_uring_op_tracking *op = &p_poll->u.io_uring_fds.op_tracking[idx];

                *flags = 0;
                
                if (is_accept) {
                    // Handle async accept completion
                    if (cqe->res >= 0) {
                        int accepted_sock = cqe->res;
                        *flags = DW_POLLIN;
                        *aux = op->aux;

                        // Store the accepted socket fd for the caller
                         op->fd = accepted_sock;
                        
                        // Create connection for the accepted socket
                        struct sockaddr_in client_addr;
                        socklen_t client_len = sizeof(client_addr);
                        if (getpeername(accepted_sock, (struct sockaddr*)&client_addr, &client_len) == 0) {
                            int new_conn_id = conn_alloc(accepted_sock, client_addr, TCP);
                            if (new_conn_id >= 0) {
                                conn_set_status_by_id(new_conn_id, READY);
                                op->conn_id = new_conn_id;
                                dw_log("Accepted connection: sock=%d -> conn_id=%d\n", accepted_sock, new_conn_id);
                            }
                        } else {
                            dw_log("getpeername() failed for accepted socket %d\n", accepted_sock);
                        }

                        p_poll->u.io_uring_fds.pending_accepts--;
                        
                        // Immediately submit a read for the new connection
                        struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
                        if (sqe) {
                            unsigned new_idx = sqe - ring->sq.sqes;
                            p_poll->u.io_uring_fds.op_tracking[new_idx].fd = accepted_sock;
                            p_poll->u.io_uring_fds.op_tracking[new_idx].aux = op->aux;
                            p_poll->u.io_uring_fds.op_tracking[new_idx].flags = DW_POLLIN;
                            p_poll->u.io_uring_fds.op_tracking[new_idx].op_type = OP_READ;
                            
                            io_uring_prep_read(sqe, accepted_sock, 
                                             p_poll->u.io_uring_fds.op_tracking[new_idx].buffer,
                                             BUF_SIZE, 0);
                            sqe->user_data = new_idx;
                            p_poll->u.io_uring_fds.n_outstanding++;
                        }
                    }
                } else if (op->op_type == OP_READ) {
                    // Handle async read completion
                    if (cqe->res > 0) {
                        *flags = DW_POLLIN;
                        *aux = op->aux;

                        // Pass the async data to the connection layer
                        int conn_id = op->conn_id;
                        if (conn_id >= 0 && conn_id < MAX_CONNS) {
                            conn_info_t *conn = conn_get_by_id(conn_id);
                            if (conn) {
                                conn->async_data = op->buffer;
                                conn->async_data_size = cqe->res;
                                dw_log("Set async data for conn_id=%d, size=%d\n", conn_id, cqe->res);
                            }
                        }

                        // The data is available in op->buffer with size cqe->res
                        // You'd need to pass this to the connection layer
                    } else if (cqe->res < 0) {
                        *flags = DW_POLLERR;
                    }
                } else if (op->op_type == OP_POLL) {
                    // Handle poll completion (fallback)
                    *flags |= (cqe->res & POLLIN) ? DW_POLLIN : 0;
                    *flags |= (cqe->res & POLLOUT) ? DW_POLLOUT : 0;
                    *flags |= (cqe->res < 0) ? DW_POLLERR : 0;
                    *aux = op->aux;
                }


                io_uring_cqe_seen(ring, cqe);
                p_poll->u.io_uring_fds.n_outstanding--;
                p_poll->u.io_uring_fds.cqe_iter++;
                
                if (*flags != 0) return 1;
                continue; // Skip to next CQE if no events
            }
            break; }
    #endif

    default:
        check(0, "Wrong dw_poll_type");
    }
    return 0;
}

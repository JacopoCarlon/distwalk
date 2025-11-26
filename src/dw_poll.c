#include <sys/socket.h>

#include "dw_poll.h"
#include "dw_debug.h"

// io_uring TODO changed
#ifdef USE_IO_URING
#include <liburing.h>
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
            int ret = io_uring_queue_init(IO_URING_ENTRIES, &p_poll->u.io_uring_fds.ring, 0);
            if (ret < 0) {
                errno = -ret;
                return -1;
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
            
            // Use the sqe index as our user_data to map back to aux
            unsigned idx = sqe - ring->sq.sqes;
            p_poll->u.io_uring_fds.aux_map[idx] = aux;
            p_poll->u.io_uring_fds.fd_map[idx] = fd;
            p_poll->u.io_uring_fds.flags_map[idx] = flags;
            
            unsigned poll_events = 0;
            if (flags & DW_POLLIN) poll_events |= POLLIN;
            if (flags & DW_POLLOUT) poll_events |= POLLOUT;
            
            io_uring_prep_poll_add(sqe, fd, poll_events);
            sqe->user_data = idx;
            
            p_poll->u.io_uring_fds.n_outstanding++;
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
                // For io_uring, we need to find and remove existing entries for this fd
                // This is a limitation of the current abstraction - io_uring doesn't have direct mod
                struct io_uring *ring = &p_poll->u.io_uring_fds.ring;

                // First, remove any existing entries for this fd
                for (unsigned i = 0; i < IO_URING_ENTRIES; i++) {
                    if (p_poll->u.io_uring_fds.fd_map[i] == fd) {
                        // Mark as removed
                        p_poll->u.io_uring_fds.fd_map[i] = -1;
                        p_poll->u.io_uring_fds.n_outstanding--;
                    }
                }

                // If new flags are non-zero, add a new entry
                if (flags != 0) {
                    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
                    if (!sqe) {
                        dw_log("No available SQEs in io_uring for mod\n");
                        return -1;
                    }

                    unsigned idx = sqe - ring->sq.sqes;
                    p_poll->u.io_uring_fds.aux_map[idx] = aux;
                    p_poll->u.io_uring_fds.fd_map[idx] = fd;
                    p_poll->u.io_uring_fds.flags_map[idx] = flags;

                    unsigned poll_events = (flags & DW_POLLIN ? POLLIN : 0) | (flags & DW_POLLOUT ? POLLOUT : 0);
                    io_uring_prep_poll_add(sqe, fd, poll_events);
                    sqe->user_data = idx;

                    p_poll->u.io_uring_fds.n_outstanding++;
                }

                // Submit the changes
                int submitted = io_uring_submit(ring);
                if (submitted < 0)
                    return -1;
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
            
            // Submit all prepared SQEs
            int submitted = io_uring_submit(ring);
            if (submitted < 0) {
                rv = -1;
                break;
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
            if (p_poll->u.io_uring_fds.cqe_iter < p_poll->u.io_uring_fds.cqe_count) {
                struct io_uring *ring = &p_poll->u.io_uring_fds.ring;
                struct io_uring_cqe *cqe = p_poll->u.io_uring_fds.cqes[p_poll->u.io_uring_fds.cqe_iter];
                unsigned idx = cqe->user_data;

                // Skip if this entry was removed/modified
                if (p_poll->u.io_uring_fds.fd_map[idx] == -1) {
                    io_uring_cqe_seen(ring, cqe);
                    p_poll->u.io_uring_fds.cqe_iter++;
                    continue;
                }

                *aux = p_poll->u.io_uring_fds.aux_map[idx];
                *flags = 0;
                *flags |= (cqe->res & POLLIN) ? DW_POLLIN : 0;
                *flags |= (cqe->res & POLLOUT) ? DW_POLLOUT : 0;
                *flags |= (cqe->res < 0) ? DW_POLLERR : 0;

                io_uring_cqe_seen(ring, cqe);
                p_poll->u.io_uring_fds.n_outstanding--;
                p_poll->u.io_uring_fds.cqe_iter++;
                return 1;
            }
            break; }
    #endif

    default:
        check(0, "Wrong dw_poll_type");
    }
    return 0;
}

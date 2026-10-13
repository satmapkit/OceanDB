/* Targeted file-cache eviction and independent residency inspection.
 * Mappings are read-only. No file-content bytes are changed. */
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

static long residency(void *mapping, size_t bytes) {
    size_t pages = (bytes + getpagesize() - 1) / getpagesize();
    unsigned char *vector = calloc(pages, 1);
    if (!vector || mincore(mapping, bytes, (void *)vector)) {
        perror("mincore");
        exit(2);
    }
    long count = 0;
    for (size_t i = 0; i < pages; i++) count += (vector[i] & 1) != 0;
    free(vector);
    return count;
}

int main(int argc, char **argv) {
    if (argc != 3 || (strcmp(argv[1], "inspect") && strcmp(argv[1], "evict"))) return 2;
    int fd = open(argv[2], O_RDONLY | O_NOFOLLOW);
    struct stat info;
    if (fd < 0 || fstat(fd, &info) || !S_ISREG(info.st_mode)) {
        perror("open/stat");
        return 2;
    }
    if (info.st_size == 0) {
        puts("{\"bytes\":0,\"before_pages\":0,\"after_pages\":0}");
        close(fd);
        return 0;
    }
    void *mapping = mmap(NULL, info.st_size, PROT_READ, MAP_SHARED, fd, 0);
    if (mapping == MAP_FAILED) { perror("mmap"); return 2; }
    long before = residency(mapping, info.st_size);
    if (!strcmp(argv[1], "evict")) {
#ifdef __APPLE__
        if (msync(mapping, info.st_size, MS_SYNC | MS_INVALIDATE)) {
            perror("msync"); return 2;
        }
#elif defined(__linux__)
        /* Dirty pages must be written back before advisory eviction. */
        if (fsync(fd)) { perror("fsync"); return 2; }
        int error = posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
        if (error) { fprintf(stderr, "posix_fadvise: %s\n", strerror(error)); return 2; }
#else
#error Unsupported cache platform
#endif
    }
    long after = residency(mapping, info.st_size);
    printf("{\"bytes\":%lld,\"before_pages\":%ld,\"after_pages\":%ld,\"page_bytes\":%d}\n",
           (long long)info.st_size, before, after, getpagesize());
    munmap(mapping, info.st_size);
    close(fd);
    return 0;
}

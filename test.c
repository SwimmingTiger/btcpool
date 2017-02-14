#include <zookeeper/zookeeper.h>
#include <zookeeper/proto.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

void my_watcher(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx) {
    printf("watcher: %d %s\n", type, path);
}

int main() {
    zhandle_t *zh = NULL;
    int stat = 0;
    char path[255] = {0};
    char buff[1024] = {0};
    int len = 1024;
    struct String_vector nodes = {0, NULL};
    int i = 0;

    zh = zookeeper_init("127.0.0.1:2181", my_watcher, 10000, NULL, NULL, 0);

    printf("----------------- Create Parent ----------------\n");
    zoo_create(zh, "/locks", NULL, -1, &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);
    zoo_create(zh, "/locks/jobmaker", NULL, -1, &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);

    stat = zoo_exists(zh, "/locks/jobmaker", 0, NULL);
    
    if (ZOK != stat) {
        printf("Zookeeper: create /locks/jobmaker failed!");
        exit(1);
    }

    printf("----------------- Create ----------------\n");
    stat = zoo_create(zh, "/locks/jobmaker/node", NULL, -1, &ZOO_READ_ACL_UNSAFE, ZOO_EPHEMERAL | ZOO_SEQUENCE, path, 255);
    printf("stat: %s -> %s\n", zerror(stat), path);

    stat = zoo_exists(zh, path, 0, NULL);

    if (ZOK != stat) {
        printf("Zookeeper: create /locks/jobmaker/node failed!");
        exit(1);
    }

    stat = zoo_get_children(zh, "/locks/jobmaker", 0, &nodes);

    if (ZOK != stat) {
        printf("Zookeeper: get children for /locks/jobmaker failed!");
        exit(1);
    }

    for (i=0; i<nodes.count; i++) {
        printf("nodes %d: %s\n", i, nodes.data[i]);
    }

    printf("------ Press Enter and Exit");
    getchar();

    zookeeper_close(zh);

    return 0;
}

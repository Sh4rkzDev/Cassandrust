# Programming Workshop

## Members

- Lucas Garcia Campos - 110099

## How to use

The steps to compile and run the program are detailed below.

### Compilation

To compile the program, run the following command:

```bash
make # or make build
```

It will create the image of the node that will be used by the docker-compose.

### Running

To run the program, run the following command:

```bash
make start
```

It will initialize all the nodes inside the docker-compose network. The nodes will be listening to ports:

- Node 1: 9042
- Node 2: 9044
- Node 3: 9046
- Node 4: 9048
- Node 5: 9050

To connect to a node, you must use respective port with the localhost address.

### Stopping

To stop the program, run the following command:

```bash
make stop
```

It will stop all the nodes inside the docker-compose network.

### Cleaning

To destroy the cluster, run the following command:

```bash
make destroy
```

## Testing

To test the program, run the following command:

```bash
make test
```

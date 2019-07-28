package kafka

import (
	"net"
)

type PooledDialer struct {
	dialer Dialer

	pool map[string]net.Conn
}

func (d *PooleadDialer) Dial(network string, address string) (*Conn, error) {
	return d.DialContext(context.Background(), network, address)
}

func (d *PooleadDialer) DialContext(ctx context.Context, network string, address string) (*Conn, error) {
	key := network + address
	if v, ok := d.pool[key]; ok {
		return v, nil
	}
	v, err := d.dialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	d.pool[key] = v
	return v, nil
}

func (d *PooleadDialer) DialLeader(ctx context.Context, network string, address string, topic string, partition int) (*Conn, error) {
}

func (d *PooleadDialer) DialPartition(ctx context.Context, network string, address string, partition Partition) (*Conn, error) {
}

func (d *PooleadDialer) LookupLeader(ctx context.Context, network string, address string, topic string, partition int) (Broker, error) {
}

func (d *PooleadDialer) LookupPartition(ctx context.Context, network string, address string, topic string, partition int) (Partition, error) {
	return LookupPartitionWithDialer(ctx, d, network, address, topic, partition)
}

func (d *PooleadDialer) LookupPartitions(ctx context.Context, network string, address string, topic string) ([]Partition, error) {
}

func (d *PooleadDialer) GetClientID() string {
	return d.dialer.GetClientID()
}

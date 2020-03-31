package consumes

func (this *BaseConsumer) ConsumerInfo() (info ConsumerInfo, err error) {
	info = ConsumerInfo{}
	info.Name = this.GetName()
	info.Id = this.number
	info.Closed = this.IsClosed()
	return
}

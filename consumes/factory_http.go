package consumes

type FactoryInfo struct {
	Name      string         `json:"name"`
	Desc      string         `json:"description"`
	EventNum  int            `json:"event_number"`
	EventCap  int            `json:"event_capacity"`
	Closed    bool           `json:"closed"`
	Consumers []ConsumerInfo `json:"consumers"`
}

func (this *Factory) FactoryInfo() (info FactoryInfo, err error) {
	info = FactoryInfo{}
	info.Name = this.name
	info.Desc = this.desc
	info.EventNum = len(this.eventsChan)
	info.EventCap = cap(this.eventsChan)
	info.Closed = this.IsClosed()
	csrInfos := make([]ConsumerInfo, 0)
	for _, csr := range this.consumer {
		if csrInfo, err := csr.ConsumerInfo(); err != nil {
			return info, err
		} else {
			csrInfos = append(csrInfos, csrInfo)
		}
	}
	info.Consumers = csrInfos
	return
}

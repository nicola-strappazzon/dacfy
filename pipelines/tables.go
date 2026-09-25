package pipelines

type Tables []Table

func (t Tables) Count() int {
	return len(t)
}

func (t Tables) First() Table {
	if t.Count() == 0 {
		return Table{}
	}

	return t[0]
}

func (t *Tables) Add(in Table) {
	(*t) = append((*t), in)
}

package gather

type Items []string

func (i Items) Count() int {
	return len(i)
}

func (i Items) IsEmpty() bool {
	return i.Count() == 0
}

func (i Items) IsNotEmpty() bool {
	return !i.IsEmpty()
}

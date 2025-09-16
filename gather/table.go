package gather

type Table struct {
	Database     string
	Name         string
	Engine       string
	Create       string
	Dependencies Dependencies
}

type Dependencies struct {
	Databases Items
	Tables    Items
}

package exec

type Config struct {
	Failfast bool
}

func (c Config) WithOptions(opts []Option) Config {
	for _, opt := range opts {
		c = opt(c)
	}
	return c
}

type Option func(Config) Config

func Failfast(on bool) Option {
	return func(c Config) Config {
		c.Failfast = on
		return c
	}
}

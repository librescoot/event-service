package seq

// RuleStats counts actual run starts (including replay) and evaluation/action
// errors. It does not count triggers suppressed by cooldown or concurrency.
type RuleStats struct {
	LastFire int64
	Errors   uint64
	Active   int
}

func (rn *Runner) RecordError(name string) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	metric := rn.metrics[name]
	metric.Errors++
	rn.metrics[name] = metric
}

func (rn *Runner) RuleStats(name string) RuleStats {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	metric := rn.metrics[name]
	metric.Active = len(rn.runs[name])
	return metric
}

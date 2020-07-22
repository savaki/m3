package tile

import (
	"testing"

	"github.com/apache/arrow/go/arrow/math"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/m3db/m3/src/dbnode/ts"
	xtime "github.com/m3db/m3/src/x/time"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDatapointRecorder(t *testing.T) {
	pool := memory.NewGoAllocator()
	recorder := newDatapointRecorder(pool)
	addPoints := func(size int) {
		for i := 0; i < size; i++ {
			recorder.record(
				ts.Datapoint{
					Value:          float64(i),
					TimestampNanos: xtime.UnixNano(i),
				},
				xtime.Microsecond,
				ts.Annotation("foobar"),
			)
		}
	}

	verify := func(rec *datapointRecord, size int) {
		ex := float64(size*(size-1)) / 2
		vals := rec.values()
		assert.Equal(t, ex, math.Float64.Sum(vals))

		require.Equal(t, size, vals.Len())
		for i := 0; i < size; i++ {
			assert.Equal(t, float64(i), vals.Value(i))
		}

		times := rec.timestamps()
		require.Equal(t, size, times.Len())
		for i := 0; i < size; i++ {
			assert.Equal(t, int64(i), times.Value(i))
		}

		annotation, isSingle := rec.annotations.SingleValue()
		require.True(t, isSingle)
		assert.Equal(t, ts.Annotation("foobar"), annotation)

		exAnns := make([]ts.Annotation, 0, size)
		for i := 0; i < size; i++ {
			exAnns = append(exAnns, ts.Annotation("foobar"))
		}

		assert.Equal(t, exAnns, rec.annotations.Values())

		unit, isSingle := rec.units.SingleValue()
		require.True(t, isSingle)
		assert.Equal(t, xtime.Microsecond, unit)

		exUnits := make([]xtime.Unit, 0, size)
		for i := 0; i < size; i++ {
			exUnits = append(exUnits, xtime.Microsecond)
		}

		assert.Equal(t, exUnits, rec.units.Values())
	}

	size := 10
	addPoints(size)

	rec := newDatapointRecord()
	recorder.updateRecord(rec)
	verify(rec, size)
	rec.release()

	size = 150000
	addPoints(size)
	recorder.updateRecord(rec)
	verify(rec, size)
	rec.release()
}

func TestDatapointRecorderChangingAnnotationsAndUnits(t *testing.T) {
	pool := memory.NewGoAllocator()
	recorder := newDatapointRecorder(pool)
	frame := SeriesBlockFrame{record: newDatapointRecord()}

	recorder.record(ts.Datapoint{}, xtime.Day, ts.Annotation("foo"))
	recorder.record(ts.Datapoint{}, xtime.Second, ts.Annotation("foo"))
	recorder.record(ts.Datapoint{}, xtime.Second, ts.Annotation("foo"))
	recorder.updateRecord(frame.record)
	a, single := frame.Annotations().SingleValue()
	require.True(t, single)
	annotationEqual(t, "foo", a)
	_, single = frame.Units().SingleValue()
	require.False(t, single)
	units := frame.Units().Values()
	assert.Equal(t, []xtime.Unit{xtime.Day, xtime.Second, xtime.Second}, units)
	frame.release()

	recorder.record(ts.Datapoint{}, xtime.Day, ts.Annotation("foo"))
	recorder.record(ts.Datapoint{}, xtime.Day, ts.Annotation("foo"))
	recorder.record(ts.Datapoint{}, xtime.Day, ts.Annotation("bar"))
	recorder.updateRecord(frame.record)
	u, single := frame.Units().SingleValue()
	require.True(t, single)
	assert.Equal(t, xtime.Day, u)
	_, single = frame.Annotations().SingleValue()
	require.False(t, single)
	annotations := frame.Annotations().Values()
	annotationsEqual(t, []string{"foo", "foo", "bar"}, annotations)
	frame.release()

	recorder.record(ts.Datapoint{}, xtime.Day, ts.Annotation("foo"))
	recorder.record(ts.Datapoint{}, xtime.Second, ts.Annotation("bar"))
	recorder.updateRecord(frame.record)
	_, single = frame.Units().SingleValue()
	require.False(t, single)
	_, single = frame.Annotations().SingleValue()
	require.False(t, single)
	units = frame.Units().Values()
	assert.Equal(t, []xtime.Unit{xtime.Day, xtime.Second}, units)
	annotations = frame.Annotations().Values()
	annotationsEqual(t, []string{"foo", "bar"}, annotations)
	frame.release()

	annotations = frame.Annotations().Values()
	assert.Equal(t, 0, len(annotations))
	units = frame.Units().Values()
	assert.Equal(t, 0, len(units))
}
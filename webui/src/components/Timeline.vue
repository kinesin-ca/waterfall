<script>
// import TimelinesChart from 'timelines-chart';

function lexsort(a, b, keyfunc) {
  const ka = keyfunc(a);
  const kb = keyfunc(b);


  let ret = 0;
  if (ka < kb) { ret = -1; }
  if (ka > kb) { ret =  1; }
  return ret;
}

const MIN_TIME="1970-01-01T00:00:00Z";
const MAX_TIME="2099-01-01T00:00:00Z";

export default {
  props: ['waterfallURL', 'refreshSeconds', 'maxDisplayIntervals'],
  data() {
    return {
      chart: null,
      data: {},
      start: MIN_TIME,
      end: MAX_TIME,
    }
  },
 
  watch: {
    refreshSeconds() {
      this.fetchTimeline();
    },
    waterfallURL() {
      this.fetchTimeline();
    },
    maxDisplayIntervals() {
      this.fetchTimeline();
    },

  },

  methods: {
    async fetchTimeline() {
      fetch(`${this.waterfallURL}/api/v1/details?max_intervals=${this.maxDisplayIntervals}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: `{ "start": "${this.start}", "end": "${this.end}" }`
      })
        .then((response) =>  {
          if (!response.ok) {
            throw new Error('Network response was not OK');
          }
          return response.json();
        })
        .then((payload) => {
          payload.map((group) => {
            group.data.sort((a, b) => lexsort(a, b, (v) => v.label));
            Object.values(group.data).map((label) => {
              label.data.map((interval) => {
                interval.timeRange = interval.timeRange.map((t) => new Date(t));
              })
            })
          });
          payload.sort((a, b) => lexsort(a, b, (v) => v.group));

          this.data = payload;
          this.chart.data(payload);
        })
        .catch(err => { throw err });
    },

    update() {
      this.fetchTimeline();
      setTimeout(() => {
        this.update();
      }, this.refreshSeconds * 1000);
    },

      setVisibleRange(dateRange) {
        if (dateRange === null) {
          this.start = MIN_TIME;
          this.end = MAX_TIME;
        } else {
          this.start = dateRange[0].toISOString();
          this.end = dateRange[1].toISOString();
        }
        this.fetchTimeline();
      }
  },

  mounted() {
    this.chart = TimelinesChart()(document.getElementById("timeline-graph"))
            .timeFormat("%Y-%m-%dT%H:%M:%S.%LZ")    // ISO 8601 format
            .zScaleLabel('State')
            .zQualitative(true)
            .useUtc(false)
            .onZoom((dateRange, _) => this.setVisibleRange(dateRange))
            .onSegmentClick((segment) => this.$emit('updateActiveSegment', segment) );
    this.update();
  },
};
</script>

<template>
  <div id="timeline-graph"></div>
</template>

<style>
  .svgicon {
    height: 1em;
    width: auto;
  }
</style>


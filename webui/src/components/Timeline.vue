<script>
// import TimelinesChart from 'timelines-chart';

export default {
  props: ['waterfallURL', 'refreshSeconds'],
  data() {
    return {
      chart: null,
      data: {},
    }
  },
 
  watch: {
    refreshSeconds() {
      this.fetchTimeline();
    },
    waterfallURL() {
      this.fetchTimeline();
    },
  },

  methods: {
    async fetchTimeline() {
      fetch(`${this.waterfallURL}/api/v1/details`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: '{ "start": "2021-09-01T00:00:00Z", "end": "2022-10-01T00:00:00Z" }'
      })
        .then((response) =>  {
          if (!response.ok) {
            throw new Error('Network response was not OK');
          }
          return response.json();
        })
        .then((payload) => {
          payload.map((group) => {
            Object.values(group.data).map((label) => {
              label.data.map((interval) => {
                interval.timeRange = interval.timeRange.map((t) => new Date(t));
              })
            })
          });
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
  },

  mounted() {
    this.chart = TimelinesChart()(document.getElementById("timeline-graph"))
            .timeFormat("%Y-%m-%dT%H:%M:%S.%LZ")    // ISO 8601 format
            .zScaleLabel('State')
            .zQualitative(true)
            .useUtc(false)
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


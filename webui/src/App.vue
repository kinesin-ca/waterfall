<script>
import Timeline from './components/Timeline.vue'
import GlobalSettings from './components/GlobalSettings.vue'
import SegmentDetails from './components/SegmentDetails.vue'

export default {
  data() {
    return {
      refreshSeconds: 1,  // How often to refresh
      waterfallURL: 'http://localhost:2503',
      activeSegment: null,
      maxDisplayIntervals: 500,
    }
  },

  methods: {
    updateURL(url) {
      this.waterfallURL = url;
    },
    updateRefreshInterval(interval) {
      this.refreshSeconds = interval;
    },
    updateMaxDisplayIntervals(cnt) {
      this.maxDisplayIntervals = cnt;
    },

    setActiveSegment(segment) {
      this.activeSegment = segment;
    },
  },

  components: {
    GlobalSettings,
    Timeline,
    SegmentDetails,
  },
};
</script>

<style>
select { max-width: 25%; }
input { max-width: 25%; }
</style>

<template>
    <GlobalSettings
      :waterfallURL="waterfallURL"
      :refreshSeconds="refreshSeconds"
      :maxDisplayIntervals="maxDisplayIntervals"
      @update-refresh-interval="(interval) => this.updateRefreshInterval(interval)"
      @update-waterfall-url="(url) => this.updateURL(url)"
      @update-max-display-intervals="(cnt) => this.updateMaxDisplayIntervals(cnt)"
      />
  <br/>
  <div>
    <Timeline
      :waterfallURL="waterfallURL"
      :refreshSeconds="refreshSeconds"
      :maxDisplayIntervals="maxDisplayIntervals"
      @update-active-segment="(segment) => this.setActiveSegment(segment)"
    />
  </div>
  <br/>
  <SegmentDetails
    v-if="this.activeSegment !== null"
    :activeSegment="activeSegment"
    />
</template>

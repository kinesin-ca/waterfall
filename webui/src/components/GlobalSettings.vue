<script>
export default {
  props: ['refreshSeconds', 'waterfallURL', 'maxDisplayIntervals'],
  data() {
    return {
      interval: this.refreshSeconds,
      url: this.waterfallURL,
      max_display_intervals: this.maxDisplayIntervals,
    };
  },
  emits: ['update-refresh-interval', 'update-waterfall-url', 'update-max-intervals'],
  computed: {
    validRefreshIntervals() {
      return [1, 5, 10, 15, 30, 60, 300, 600];
    },
    validDisplayIntervals() {
      return [0, 100, 250, 500, 1000, 1500];
    },
    isSelected(interval) {
      return (interval === this.refreshSeconds ? 'selected' : 'unselected');
    },
  },
};
</script>

<template>
  <details>
    <summary>Global Settings</summary>
    <label>
      Waterfall Base URL
      <input @change="$emit('update-waterfall-url', url)" v-model="url"/>
    </label>
    <label>
      Refresh Interval (seconds)
      <select @change="$emit('update-refresh-interval', interval)" v-model="interval">
        <option v-for="interval in validRefreshIntervals"
                :key="interval"
                :value="interval"
                >
                {{ interval }} Seconds
        </option>
      </select>
    </label>
    <label>
      Max Display Intervals
      <select @change="$emit('update-max-display-intervals', max_display_intervals)" v-model="max_display_intervals">
        <option v-for="cnt in validDisplayIntervals"
                :key="cnt"
                :value="cnt"
                >
                {{ cnt }} Segments
        </option>
      </select>
    </label>

  </details>
</template>

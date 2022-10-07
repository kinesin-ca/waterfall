<script>
export default {
  props: ['title', 'sorted', 'ascending', 'sortable'],
  emits: ['update-sort-column'],
  computed: {
    src() {
      let loc = '/icon-';
      if (this.sorted) {
        if (this.ascending) {
          loc += 'sort-ascending';
        } else {
          loc += 'sort-decending';
        }
      } else {
        loc += 'order-vertical';
      }
      loc += '.svg';
      return loc;
    },
  },
};
</script>

<style>
  .svgicon {
    height: 1em;
    width: auto;
  }
</style>

<template>
  <div>
    {{ title }}
    <a href="#">
      <img
        v-if="sortable"
        class='svgicon'
        :src="src"
        :alt="title"
        @click="$emit('update-sort-column')"/>
    </a>
  </div>
</template>

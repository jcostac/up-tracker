<template>
<v-tooltip :location="leftTooltipLocation" :text="leftTooltipText">
    <template v-slot:activator="{ props }">
        <v-btn class="graph-btns" :icon="leftIcon" variant="text" v-bind="props" @click="handleLeftIconClick">
        </v-btn>
    </template>
</v-tooltip>
<v-tooltip :location="rightTooltipLocation" :text="rightTooltipText">
    <template v-slot:activator="{ props }">
        <v-btn class="graph-btns" :icon="rightIcon" variant="text" v-bind="props" @click="handleRightIconClick">
        </v-btn>
    </template>
</v-tooltip>
</template>

<script>
export default {
    name: 'IconosGrafica',
    props: {
        leftIcon: {
            type: String,
            default: 'mdi-download'
        },
        leftTooltipText: {
            type: String,
            default: 'default text left'
        },
        leftTooltipLocation: {
            type: String,
            default: 'start'
        },
        rightIcon: {
            type: String,
            default: 'mdi-plus'
        },
        rightTooltipText: {
            type: String,
            default: 'default text right'
        },
        rightTooltipLocation: {
            type: String,
            default: 'end'
        }
    },
    methods: {
        handleLeftIconClick() {
            this.$emit('icon-clicked', 'left'); // Emit event for left icon
        },
        handleRightIconClick() {
            this.$emit('icon-clicked', 'right'); // Emit event for right icon
        }
    }
}
</script>

<style scoped>
.graph-btns {
    margin: 0 5px;
    color: teal;
    transform: scale(1.15); 
    transition: all 0.5s;
}

.graph-btns:hover {
    transform: scale(1); 
}


</style>        

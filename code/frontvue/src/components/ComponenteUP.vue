<template>
<v-container class="main-container">
    <transition name="slide-fade" mode="in-out">
        <!-- FRONT CARD -->
        <v-card class="card-content" v-if="isFrontActive" key="front">
            <v-card-title class="text-h5 tarjeta d-flex justify-space-between align-center">
                <i>CONSULTAS POR UNIDADES DE PROGRAMACIÓN</i>

                <v-row justify="end">
                    <IconosGrafica leftTooltipText="Descargar datos" rightTooltipText="Abrir resumen" @icon-clicked="handleIconClick" /> <!-- event handler to activate back card-->
                </v-row>

            </v-card-title>
            <SubtituloTarjeta :selectedView="selectedView" :class="{ 'shake': isShaking }" />

            <v-card-text class="tarjeta">
                <v-row class="mb-4">
                    <v-col cols="4">
                        <v-row cols="12" justify="center" class="mb-4">
                            <v-btn-toggle color="teal" v-model="selectedView" mandatory rounded="lg" variant="outlined" class="button-toggle" :class="{ 'shake': isShaking }">
                                <v-btn value="PROGRAMAS" @click="onViewChanged">Programas</v-btn>
                                <v-btn value="GANANCIAS" @click="onViewChanged">Ganancias</v-btn>
                            </v-btn-toggle>
                        </v-row>

                        <transition name="fade" appear>
                            <v-container v-if="selectedView !== null">
                                <v-col cols="12" align-self="d-flex flex-column justify-space-between">
                                    <div style="margin-bottom: 40px;">
                                        <v-row class="filters">
                                            <v-label>Fecha inicial</v-label>
                                            <VueDatePicker model-type="yyyy-MM-dd" v-model="entradaAPI.fecha_inicial" :format="'yyyy-MM-dd'" text-input></VueDatePicker>
                                        </v-row>

                                        <v-row class="filters">
                                            <v-label>Fecha final</v-label>
                                            <VueDatePicker color="teal" model-type="yyyy-MM-dd" v-model="entradaAPI.fecha_final" :format="'yyyy-MM-dd'" text-input />
                                        </v-row>
                                    </div>
                                    <v-row class="filters">
                                        <v-select label="Mercado" v-model="entradaAPI.mercado" :items="listaMercados" @update:modelValue="onMercadoChange" clearable outlined></v-select>
                                    </v-row>
                                    <v-row class="filters">
                                        <v-autocomplete label="UP" v-model="entradaAPI.up" :items="listaUPs" placeholder="Seleccione una UP" clearable filterable outlined :loading="loadingUP"></v-autocomplete>
                                    </v-row>
                                    <v-row class="filters">
                                        <v-select v-model="entradaAPI.sentido" :items="listaSentidos" label="Sentido" :disabled="disableFilter" placeholder="Seleccione un sentido" clearable multiple outlined></v-select>
                                    </v-row>
                                    <v-row class="filters">
                                        <v-select label="Agrupar por" :items="agruparOptions" v-model="entradaAPI.agrupar" @update="(val) => { console.log('Agrupar changed:', val); }" outlined placeholder="Seleccione una agrupación temporal"></v-select>
                                    </v-row>
                                    <v-row justify="center">
                                        <v-btn class="consulta-btn" rounded="lg" variant="elevated" color="teal" @click="actualizarConsulta()">Obtener</v-btn>
                                    </v-row>
                                </v-col>
                            </v-container>
                        </transition>
                    </v-col>

                    <v-col cols="8" class="d-flex justify-center align-center" v-if="selectedView !== null">
                        <div ref="chart" style="width: 100%; height: 100%;"></div>
                    </v-col>

                </v-row>
            </v-card-text>
        </v-card>
    </transition>

    <transition name="slide-fade" mode="out-in">
        <!-- BACK CARD -->
        <v-card class="card-content" v-if="isBackActive" key="back">
            <v-card-title class="text-h5 tarjeta d-flex justify-space-between align-center">
                <i>RESUMEN DE LA CONSULTA</i>
                <v-row justify="end">
                    <IconosGrafica rightIcon="mdi-close" rightTooltipText="Cerrar resumen" @icon-clicked="handleIconClick" />
                </v-row>
            </v-card-title>
            <SubtituloTarjeta ref="subtitle" :selectedView="selectedView" text="Datos de resumen para consultas de" :class="{ 'shake': isShaking }" />

            <v-card-text class="tarjeta">
                <v-row class="mb-4">
                    <v-col cols="12" align="center">
                        <v-table>
                            <thead>
                                <tr>
                                    <th v-for="header in summaryTableHeaders" :key="header.value">
                                        {{ header.text }}
                                    </th>
                                </tr>
                            </thead>
                            <tbody>
                                <tr v-for="item in summaryTableItems" :key="item.statistic">
                                    <td>{{ item.statistic }}</td>
                                    <td v-for="header in summaryTableHeaders" :key="header.value">
                                        {{ item[header.value] }}
                                    </td>
                                </tr>
                            </tbody>
                        </v-table>
                    </v-col>
                </v-row>

            </v-card-text>
        </v-card>
    </transition>
</v-container>
</template>

<script>
import axios from 'axios';
//import store from '../store';
import '@vuepic/vue-datepicker/dist/main.css';
import IconosGrafica from './IconosGrafica.vue';
import * as echarts from 'echarts';
import SubtituloTarjeta from './SubtituloTarjeta.vue';
import {
    useToast
} from "vue-toastification";
import { saveAs } from 'file-saver';

export default {
    components: {
        IconosGrafica,
        SubtituloTarjeta
    },

    data() {
        return {
            entradaAPI: {
                fecha_inicial: "", //yyyy-MM-dd
                fecha_final: "", //yyyy-MM-dd
                up: [], //string[]
                mercado: "", //string
                sentido: [], //string[]
                agrupar: "" //string

            },

            listaUPs: [],

            listaMercados: [
                "PBF",
                "PVP",
                "PHF1",
                "PHF2",
                "PHF3",
                "PHF4",
                "PHF5",
                "PHF6",
                "PHF7",
                "P48",
                "aFRR",
                "mFRR",
                "Desvios",
                "RR"
            ],

            listaSentidos: ["Subir", "Bajar"],

            agruparOptions: ["Hora", "Dia", "Mes", "Año"],

            selectedView: null, //view selected by the user

            disableFilter: false, // variable to disable sentido filter depending on mercado sesion

            isBackActive: false, //back card is not active

            isFrontActive: true, //front card is active

            isShaking: false, //shake animation

            loadingUP: false, //loading up list

            responseData: null, //response data from the API

            summaryTableHeaders: [], //headers for the summary table

            summaryTableItems: [], //items for the summary table

        };
    },

    watch: {
        selectedView: {
            handler(newValue) {
                if (newValue !== null) {
                    this.$nextTick(() => {
                        this.iniciar_grafica();
                    });
                }
            },
            immediate: true
        },
        'entradaAPI.fecha_inicial': function () {
            this.onMercadoChange();
        },
        'entradaAPI.fecha_final': function () {
            this.onMercadoChange();
        },
    },

    mounted() {
        this.iniciar_grafica();
        window.addEventListener('resize', this.resizeChart); // Add resize event listener
    },

    beforeUnmount() {
        window.removeEventListener('resize', this.resizeChart); // Clean up event listener
    },

    setup() {
        const toast = useToast();
        return {
            toast
        }
    },

    methods: {
        iniciar_grafica() {
            if (this.$refs.chart) {
                if (this.chart) {
                    this.chart.dispose();
                }
                this.chart = echarts.init(this.$refs.chart);
                this.option = {
                    xAxis: {
                        type: 'category',
                        data: []
                    },
                    yAxis: {
                        type: 'value'
                    },
                    series: [{
                        data: [],
                        type: 'line'
                    }]
                };
                this.chart.setOption(this.option);
            }
        },

        graficarDatos(serie_temporal) {
            let x = [];
            let y = [];
            console.log(serie_temporal);


            for (let i = 0; i < serie_temporal.length; i++) { //iterate through the serie_temporal array
                if (this.selectedView.toLowerCase() === 'programa') {
                    x.push(serie_temporal[i]['FECHA']);
                    y.push(serie_temporal[i]['ENERGIA']);
                } else if (this.selectedView.toLowerCase() === 'ganancia') {
                    x.push(serie_temporal[i]['FECHA']);
                    y.push(serie_temporal[i]['GANANCIA']);
                } else {
                    x.push(serie_temporal[i]['FECHA']);
                    y.push(serie_temporal[i]['PRECIO']);
                }
            }

            let option = {
                xAxis: {
                    type: 'category',
                    data: x,
                    axisLabel: {
                        formatter: function (value) {
                            return echarts.format.formatTime('yyyy-MM-dd', new Date(value));
                        }
                    }
                },
                yAxis: {
                    type: 'value'
                },
                series: [{
                    data: y,
                    type: 'line'
                }]
            };

            this.chart.setOption(option);
        },

        async obtenerDatos() {
            try {
                let mercadoType = this.entradaAPI.mercado.toLowerCase();
                let consultaType = this.selectedView.toLowerCase();

                console.log("consultaType: ", consultaType, "mercadoType: ", mercadoType);

                const response = await axios({
                    method: 'post',
                    url: `${process.env.VUE_APP_API_BASE_URL}/up/${consultaType}/${mercadoType}`,
                    withCredentials: false,
                    data: { entradaAPI: this.entradaAPI },
                    headers: {
                        'Access-Control-Allow-Origin': '*',
                        'Content-Type': 'application/json'
                    },
                });

                this.cargando = false;
                this.responseData = response.data;
                let serie_temporal = this.responseData;
                console.log(serie_temporal);
                return serie_temporal; // Return the serie_temporal
            } catch (error) {
                console.error('Error fetching data:', error);
                this.toast.error("Failed to fetch data. Please try again later.");
                return null; // Return null in case of error
            }
        },

        async fetchUPList() {
            this.loadingUP = true;
            try {
                const response = await axios({
                    method: 'get',
                    url: `${process.env.VUE_APP_API_BASE_URL}/up/get-list`,
                    params: {
                        fecha_inicial: this.entradaAPI.fecha_inicial,
                        fecha_final: this.entradaAPI.fecha_final,
                        mercado: this.entradaAPI.mercado
                    },
                    headers: {
                        //Authorization: "Bearer " + store.state.token, 
                        'Access-Control-Allow-Origin': '*',
                        'Content-Type': 'application/json'
                    },
                });

                if (response.data.status === 'success') {
                    this.listaUPs = response.data.data.up_list; // get lista up data from the "up_list" key in the response
                } else {
                    console.error('Error fetching UP list:', response.data.message); // log error message
                    this.listaUPs = [];
                }
            } catch (error) {
                console.error('Error fetching UP list:', error);
                this.listaUPs = [];
                this.toast.error("Failed to fetch UP list. Please try again later.");
            } finally {
                this.loadingUP = false;
            }
        },

        async actualizarConsulta() {
            // if disable filter is false, then all fields must be filled before allowing the button to be clicked
            if (this.disableFilter === false) {
                if (this.entradaAPI.mercado && this.entradaAPI.fecha_inicial && this.entradaAPI.fecha_final && this.entradaAPI.up && this.entradaAPI.sentido && this.entradaAPI.agrupar) {
                    const serie_temporal = await this.obtenerDatos(); //get the serie_temporal data from the API
                    if (serie_temporal) { //if serie_temporal is not null, then update the chart
                        if (this.chart) {
                            this.graficarDatos(serie_temporal);
                        }
                    }
                } else { // if any field is not filled, then show error message
                    this.toast.error("Please fill all required fields before submitting your request.");
                }
            } else { // if disable filter is true, then only the mercado, fecha_inicial, fecha_final, up and agrupar
                if (this.entradaAPI.mercado && this.entradaAPI.fecha_inicial && this.entradaAPI.fecha_final && this.entradaAPI.up && this.entradaAPI.agrupar) {
                    const serie_temporal = await this.obtenerDatos(); //get the serie_temporal data from the API
                    if (serie_temporal) { //if serie_temporal is not null, then update the chart
                        if (this.chart) {
                            this.graficarDatos(serie_temporal);
                        }
                    }
                } else { // if any field is not filled, then show error message
                    this.toast.error("Please fill all required fields before submitting your request.");
                }
            }
        },

        onMercadoChange() {
            // Call the method to disable the sentido filter
            this.disableSentidoFilter(); //method checks the mercado and sets disableFilter to true if the mercado is not valid

            if (this.entradaAPI.mercado && this.entradaAPI.fecha_inicial && this.entradaAPI.fecha_final) {
                this.fetchUPList();
            } else {
                this.listaUPs = [];
            }
        },

        disableSentidoFilter() {
            // Logic to disable the sentido filter based on the selected mercado
            this.disableFilter = this.entradaAPI.mercado === 'PBF' ||
                this.entradaAPI.mercado === 'PVP' ||
                this.entradaAPI.mercado === 'PHF1' ||
                this.entradaAPI.mercado === 'PHF2' ||
                this.entradaAPI.mercado === 'PHF3' ||
                this.entradaAPI.mercado === 'PHF4' ||
                this.entradaAPI.mercado === 'PHF5' ||
                this.entradaAPI.mercado === 'PHF6' ||
                this.entradaAPI.mercado === 'PHF7' ||
                this.entradaAPI.mercado === 'P48';
        },

        changeLog() {
            console.log('Dropdown selection changed API input: ', this.entradaAPI.sentido); //verify dropdown selection changed
        },

        onViewChanged() {
            console.log('Selected view: ', this.selectedView); // Add this line to verify the method is called
            // No need to call iniciar_grafica here, the watcher will handle it
        },

        handleIconClick(icon) {
            if (this.selectedView === null) {
                this.triggerShake();
                return; // Exit the method early
            }
            if (icon === 'left') {
                console.log('Download data'); //cambiar por descargar datos
                this.downloadData(); //call the method to download the data
            } else if (icon === 'right') {
                console.log('Opened summary'); //cambiar por abrir resumen
                this.isFrontActive = !this.isFrontActive; // toggle the opposite state i.e. activated the card that is not active
                this.isBackActive = !this.isBackActive;
                if (this.responseData) {
                    this.tablaResumen(); //call the method to obtain the summary
                } else {
                    if (this.isBackActive == true) {
                        this.toast.error("No data to show, please pass a query first");
                    }
                }
            }
        },

        async obtenerResumen() {
            try {
                const response = await axios({
                    method: 'post',
                    url: `${process.env.VUE_APP_API_BASE_URL}/up/get-summary`,
                    withCredentials: false,
                    data: { entradaAPI: this.entradaAPI },
                    headers: {
                        //Authorization: "Bearer " + store.state.token,
                        'Access-Control-Allow-Origin': '*',
                        'Content-Type': 'application/json'
                    },
                });

                this.responseData = response.data;
                let summaryData = this.responseData;
                console.log(summaryData);
                return summaryData;
            } catch (error) {
                console.error('Error fetching summary:', error);
                this.toast.error("Failed to fetch summary. Please try again later.");
            }

        },

        async tablaResumen() {
            const summaryData = await this.obtenerResumen();
            if (summaryData && summaryData.summary_stats) {
                const stats = summaryData.summary_stats;
                
                // Define fixed columns
                const fixedColumns = ['ENERGIA', 'PRECIO', 'GANANCIAS'];
                
                // Define table headers
                this.summaryTableHeaders = fixedColumns.map(col => ({ text: col, value: col }));

                //creates the following table headers 
                //[
                //    { text: 'ENERGIA', value: 'ENERGIA' },
                //    { text: 'PRECIO', value: 'PRECIO' },
                //    { text: 'GANANCIAS', value: 'GANANCIAS' }
                //]
                
                // Define statistic rows
                const statRows = ['count', 'mean', 'std', 'min', '25%', '50%', '75%', 'max'];
                
                // Create table items
                this.summaryTableItems = statRows.map(stat => {
                    const row = { statistic: stat };
                    fixedColumns.forEach(col => {
                        row[col] = stats[col] && stats[col][stat] !== undefined ? stats[col][stat] : 'N/A';
                    });
                    return row;
                });
            } else {
                console.error('Invalid summary data structure');
                this.toast.error("Failed to process data for summary.");
            }
        },

        downloadData() {
            if (!this.responseData) {
                this.toast.error("No data to download, please pass a query first");
                return;
            }

            const data = JSON.stringify(this.responseData, null, 2);
            const blob = new Blob([data], { type: 'application/json' });
            const filename = `data_${this.selectedView}_${this.entradaAPI.mercado}_${this.entradaAPI.fecha_inicial}_${this.entradaAPI.fecha_final}.json`;
            saveAs(blob, filename);
            this.toast.success("Data downloaded successfully");
        },

        resizeChart() {
            if (this.chart) {
                this.chart.resize(); // Resize the chart
            }
        },

        triggerShake() {
            this.isShaking = true;
            setTimeout(() => {
                this.isShaking = false;
            }, 500); // Match duration of the animation
        },
    },
}
</script>

<style scoped>
/* FILTROS */
.filters {
    margin-bottom: 10px;
}

/* BOTON OBTENER*/
.consulta-btn {
    transition: all 0.5s ease;
    /* Adjust duration as needed */
}

.consulta-btn:hover {
    transform: scale(0.85);
}

/* FRONT AND BACK CARDS */
.card-content {
    width: 100%;
    background-color: #FAFAF5;
    padding: 20px;
    transition: all 0.3s ease;
    max-height: 100%;
    /* Ensure it doesn't exceed container height */
}

/* SLIDE FADE TRANSITIONS */
.slide-fade-enter-active {
    transition: all 0.8s ease-in-out;
}

.slide-fade-leave-active {
    transition: all 0.3s ease-in-out;
}

.slide-fade-enter-from,
.slide-fade-leave-to {
    transform: translateX(-30px), translateY(-30px);
    opacity: 0;
}

/*MAIN CONTAINER*/
.main-container {
    display: flex;
    flex-direction: column;
    height: 100%;

}

/*FADE TRANSITIONS*/
.fade-enter-active,
.fade-leave-active {
    transition: all 2s ease;
}

.fade-enter-from,
.fade-leave-to {
    opacity: 0;
}

/* SHAKE ANIMATION */
@keyframes shake {
    0% {
        transform: translate(0);
    }

    25% {
        transform: translate(-8px);
    }

    50% {
        transform: translate(8px);
    }

    75% {
        transform: translate(-8px);
    }

    100% {
        transform: translate(0);
    }
}

.shake {
    animation: shake 0.5s ease;
}
</style>





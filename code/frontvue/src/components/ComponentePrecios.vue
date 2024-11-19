<template>
<v-container class="main-container">
    <transition name="slide-fade" mode="in-out">
        <!-- FRONT CARD -->
        <v-card class="card-content" v-if="isFrontActive" key="front">
            <v-card-title class="text-h5 tarjeta d-flex justify-space-between align-center">
                <i>CONSULTAS DE PRECIO</i>

                <v-row justify="end">
                    <IconosGrafica leftTooltipText="Descargar datos" rightTooltipText="Abrir resumen" @icon-clicked="handleIconClick"/>
                </v-row>
            </v-card-title>
            <SubtituloTarjeta :selectedView="selectedView" :class="{ 'shake': isShaking }" text="Consultando datos para mercados de"/>
            
            <v-card-text class="tarjeta">
                <v-row class="mb-4">
                    <v-col cols="4">
                        <v-row cols="12" justify="center" class="mb-4">
                            <v-btn-toggle v-model="selectedView" mandatory rounded="lg" variant="outlined" class="button-toggle" :class="{ 'shake': isShaking }">
                                <v-btn value="ENERGIA" @click="onViewChanged">Energía</v-btn>
                                <v-btn value="AJUSTE" @click="onViewChanged">Ajuste</v-btn>
                            </v-btn-toggle>
                        </v-row>

                        <transition name="fade">
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
                                    <v-select
                                        label="Mercado"
                                        v-model="entradaAPI.mercado"
                                        :items="selectedView === 'ENERGIA' ? listaMercadosEnergia : listaMercadosAjuste"
                                        @update:modelValue="disableSentidoFilter"
                                        clearable
                                        outlined
                                    ></v-select>
                                </v-row>
                                <v-row class="filters">
                                    <v-select v-model="entradaAPI.programa" 
                                    :items="listaSentidos" 
                                    label="Sentido" 
                                    :disabled="disableFilter"
                                    placeholder="Seleccione un sentido"
                                    @update:modelValue="changeLog"
                                    clearable
                                    multiple
                                    outlined
                                    ></v-select>
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

    <transition name="slide-fade" mode="in-out">
        <!-- BACK CARD -->
        <v-card class="card-content" v-if="!isFrontActive" key="back">
            <v-card-title class="text-h5 tarjeta d-flex justify-space-between align-center">
                <i>RESUMEN DE PRECIOS</i>
                <v-row justify="end">
                    <IconosGrafica rightIcon="mdi-close" rightTooltipText="Cerrar resumen" @icon-clicked="handleIconClick"/>
                </v-row>
            </v-card-title>
            <SubtituloTarjeta ref="subtitle" :selectedView="selectedView" text="Datos de resumen para consultas de mercados de" :class="{ 'shake': isShaking }" />

            <v-card-text class="tarjeta">
                <v-row class="mb-4">
                    <v-col cols="12" align="center">
                        INSERTAR TABLE CON DATOS DE RESUMEN
                    </v-col>
                </v-row>
            </v-card-text>
        </v-card>
    </transition>
</v-container>
</template>

<script>
import axios from 'axios';
import '@vuepic/vue-datepicker/dist/main.css';
import IconosGrafica from './IconosGrafica.vue';
import * as echarts from 'echarts';
import SubtituloTarjeta from './SubtituloTarjeta.vue';
import { useToast } from "vue-toastification";
import { saveAs } from 'file-saver';

export default {
    components: {
        IconosGrafica,
        SubtituloTarjeta
    },

    setup() {
        const toast = useToast();
        return { toast }
    },

    data() {
        return {
            entradaAPI: {
                fecha_inicial: "",
                fecha_final: "",
                mercado: "",
                up: null,
                sentido: [],
                agrupar: ""
            },

            listaMercadosEnergia: [
                "Diario",
                "Intradiario", 
                "Restricciones",
            ],

            listaMercadosAjuste: [
                "aFRR banda",
                "mFRR",
                "RR",
                "Desvios"
            ],

            listaSentidos: ["Subir", "Bajar"],

            agruparOptions: ["Hora", "Dia", "Semana", "Mes", "Año"],

            selectedView: null,

            disableFilter: false,

            isFrontActive: true,

            isShaking: false,

            isBackActive: false,

            responseData: null,
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
        }
    },

    mounted() {
        this.iniciar_grafica();
        window.addEventListener('resize', this.resizeChart);
    },

    beforeUnmount() {
        window.removeEventListener('resize', this.resizeChart);
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

            for (let i = 0; i < serie_temporal.length; i++) {
                x.push(serie_temporal[i]['FECHA']);
                y.push(serie_temporal[i]['ENERGIA']);
            }

            let option = {
                xAxis: {
                    type: 'category',
                    data: x,
                    axisLabel: {
                        formatter: function(value) {
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
                    url: `${process.env.VUE_APP_API_BASE_URL}/precios/${mercadoType}`,
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
                if (this.chart) {
                    this.actualizar_grafica(serie_temporal);
                }
            } catch (error) {
                console.error('Error fetching data:', error);
                this.toast.error("Failed to fetch data. Please try again later.");
            }
        },

        async actualizarConsulta() {
            if (this.disableFilter === false) {
                if (this.entradaAPI.mercado && this.entradaAPI.fecha_inicial && this.entradaAPI.fecha_final && this.entradaAPI.agrupar) {
                    const serie_temporal = await this.obtenerDatos();
                    if (serie_temporal) {
                        this.graficarDatos(serie_temporal);
                    }
                } else {
                    this.toast.error("Please fill all required fields before submitting your request.");
                }
            } else {
                if (this.entradaAPI.mercado && this.entradaAPI.fecha_inicial && this.entradaAPI.fecha_final && this.entradaAPI.agrupar && this.entradaAPI.sentido) {
                    const serie_temporal = await this.obtenerDatos();
                    if (serie_temporal) {
                        this.graficarDatos(serie_temporal);
                    }
                } else {
                    this.toast.error("Please fill all required fields before submitting your request.");
                }
            }
        },

        disableSentidoFilter() {
            console.log('checkMercado called');
            this.disableFilter = this.entradaAPI.mercado === 'Diario' || this.entradaAPI.mercado === 'Intradiario' || this.entradaAPI.mercado === 'aFRR banda';
        },

        changeLog() {
            console.log('Dropdown selection changed API input: ', this.entradaAPI.programa);
        },

        onViewChanged() {
            console.log('Selected view: ', this.selectedView);
        },

        handleIconClick(icon) {
            if (this.selectedView === null) {
                this.triggerShake();
                return;
            }
            if (icon === 'left') {
                console.log('Download data');
                this.downloadData();
            } else if (icon === 'right') {
                console.log('Open summary');
                this.isFrontActive = !this.isFrontActive;
                this.isBackActive = !this.isBackActive;
                this.obtenerResumen();
            }
        },

        async obtenerResumen() {
            try {
                const response = await axios({
                    method: 'post',
                    url: `${process.env.VUE_APP_API_BASE_URL}/api/mercado/get-summary`,
                    withCredentials: false,
                    data: { entradaAPI: this.entradaAPI },
                    headers: {
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
            console.log(summaryData);
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
                this.chart.resize();
            }
        },

        triggerShake() {
            this.isShaking = true;
            setTimeout(() => {
                this.isShaking = false;
            }, 500);
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
.consulta-btn{
    transition: all 0.5s ease;
}

.consulta-btn:hover{
   transform: scale(0.85);
}

/* FRONT AND BACK CARDS */
.card-content {
    width: 100%;
    background-color: #FAFAF5;
    padding: 20px;
    transition: all 0.3s ease;
    max-height: 100%;
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
.fade-enter-active, .fade-leave-active {
  transition: all 2s ease;
}
.fade-enter-from, .fade-leave-to {
  opacity: 0;
}

/* SHAKE ANIMATION */
@keyframes shake {
    0% { transform: translate(0); }
    25% { transform: translate(-8px); }
    50% { transform: translate(8px); }
    75% { transform: translate(-8px); }
    100% { transform: translate(0); }
}

.shake {    
    animation: shake 0.5s ease;
}

/*TOGGLE BUTTONS*/
.v-btn-toggle .v-btn.v-btn--active {
    background-color: teal;
    color: white;
    transition: background-color 0.7s ease-in-out;
}

.v-btn-toggle .v-btn {
    color: teal;
    border-color: teal;
}

.button-toggle {
    margin-bottom: 20px;
}
</style>

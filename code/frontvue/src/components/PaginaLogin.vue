<template>
    <v-app>
        <v-content>
            <v-container class="d-flex align-center justify-center" style="height: 100vh;">
                <v-card class="elevation-12" width="300">

                        <br><br>
                        <center>
                        <v-img
                            :src="require('../assets/logo.png')" 
                            alt="Logo"
                            max-width="150"
                        ></v-img>
                        </center>
                        <br>

                    <v-card-title class="d-flex justify-center">

                    </v-card-title>
                    <!--
                    <v-toolbar dark color="primary">
                        <v-toolbar-title>Webapp</v-toolbar-title>
                    </v-toolbar>
                    -->
                    <v-card-text>
                        <v-form @submit.prevent="login">
                            <v-text-field
                                v-model="email"
                                label="Usuario"
                                required
                            ></v-text-field>
                            <v-text-field
                                v-model="password"
                                label="Contraseña"
                                type="password" 
                                required
                            ></v-text-field>
                            <br>
                            <v-btn
                                color="teal"
                                class="mr-4"
                                type="submit"
                            >
                                Iniciar sesión
                            </v-btn>
                        </v-form>
                    </v-card-text>
                </v-card>
            </v-container>
        </v-content>
    </v-app>
</template>

<script>
//import axios from 'axios';
import { mapGetters } from 'vuex';
//import axios from '@/axios';

export default {
name: 'PaginaLogin',
components: {

      },

    data() {
        return {   
            email: '',
            password: ''       
        };
      },

    mounted() {
        //const globalProps = this.$appContext.config.globalProperties;
        // Acceder a las globalProperties
        console.log("aqui")
        
    },

    computed: {
    ...mapGetters(['getMiVariableGlobal'])
    },


    methods: {


        login(){

            // De monento entramos sin hacer login            
            this.$store.dispatch('actualizarUsuarioAutenticado', true);
            this.$store.dispatch('actualizarToken', "TOKENxxxxxxxxxxxxxxx");                    
            this.$store.dispatch('actualizarUsuarioNombre', "Nombre usuario");                    
            this.$router.push('/');            



            /*
            axios(
                {
                    method: 'get',
                    url: '/resultado/login',
                    auth: {
                        username: this.email,
                        password: this.password
                      }
                }
              )
              .then((response) => {    

                console.log(JSON.stringify(response.data))

                if (response.data.status != "success"){

                    console.log("ERROR")
                    this.$swal(response.data.message);
  
                }else{
  
                    console.log("Estas validado!")                                        
                    this.$store.dispatch('actualizarUsuarioAutenticado', true);
                    this.$store.dispatch('actualizarToken', response.data.token);                    
                    this.$store.dispatch('actualizarUsuarioNombre', response.data.nombre);
                    
                    console.log("A la página de home...")

                    this.$router.push('/');
              
                }  
                  
              })
              

            */
        }

    }      
}


</script>
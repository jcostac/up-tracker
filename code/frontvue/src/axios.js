import axios from 'axios';

// crear una instancia de axios, con base url de la API de desarollo
const axiosInstance = axios.create({
  baseURL: process.env.VUE_APP_API_BASE_URL,
});

// Add response interceptor to handle errors globally
axiosInstance.interceptors.response.use(
  response => response, // Return the response if successful
  error => {
    console.error('Error response:', error.response); // Log the error
    return Promise.reject(error); // Reject the promise with the error
  }
);

// Add request interceptor to include authentication token
//axiosInstance.interceptors.request.use(
  //config => {
    //const token = localStorage.getItem('token'); // Get token from local storage
    //if (token) {
      //config.headers.Authorization = `Bearer ${token}`; // Add token to headers
    //}
    //return config; // Return the modified config
  //}
//);


//exportar la instancia de axios
export default axiosInstance;

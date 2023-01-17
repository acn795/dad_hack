import { LitElement, html, css, unsafeCSS, PropertyValueMap } from "lit";
import { customElement, state } from "lit/decorators.js";

import CountedBikes from "./assets/counted_bikes.png"
import Weather from "./assets/weather.png"
import FuelPrice from "./assets/fueal_price.png"
import CorrelationBikeVSDiesel from "./assets/correlation_bike_vs_diesel.png"
import CorrelationBikeVSe5 from "./assets/correlation_bike_vs_e5.png"
import CorrelationBikeVSe10 from "./assets/correlation_bike_vs_e10.png"
import CorrelationBikeVSRain from "./assets/correlation_bike_vs_rain.png"
import CorrelationBikeVSWind from "./assets/correlation_bike_vs_wind.png"
import CorrelationBikeVSTemperature from "./assets/correlation_bike_vs_temperature.png"

@customElement("app-root")
export class AppRoot extends LitElement {
  counter = 1;

  firstUpdated(): void {    
    setInterval(() => {
      this.requestUpdate();
    }, 1_000);
  }

  render() {
    this.counter++;

    return html`
      <div class="header">DAD - Monitoring</div>

      <div class="subheader">Basedata</div>
      <div class="row">
        ${this.renderCard("Bike-Count", CountedBikes)}
        ${this.renderCard("Weather", Weather)}
        ${this.renderCard("Fuel-Price", FuelPrice)}
      </div>

      <div class="subheader">"Fuel-Price" - "Bike-Count" correlations</div>
      <div class="row">
        ${this.renderCard("Diesel", CorrelationBikeVSDiesel)}
        ${this.renderCard("E5", CorrelationBikeVSe5)}
        ${this.renderCard("E10", CorrelationBikeVSe10)}
      </div>

      <div class="subheader">"Weather" - "Bike-Count" correlations</div>
      <div class="row">
        ${this.renderCard("Rain", CorrelationBikeVSRain)}
        ${this.renderCard("Wind", CorrelationBikeVSWind)}
        ${this.renderCard("Temperature", CorrelationBikeVSTemperature)}
      </div>
    `;
  }

  renderCard(title: string, url: string) {
    return html`
      <div class="card">
        <img class="plot" src=${url}>
        <div class="title">${title}</div>
      </div>
    `;
  }

  static styles = css`
    :host {
      display: flex;
      flex-direction: column;
      gap: 1rem;
      padding: 1rem;
      font-family: sans-serif;
      overflow: auto;
      height: 100%;
    }

    .header {
      font-size: 2.5rem;
    }

    .subheader {
      font-size: 1.5rem;
    }

    .row {
      display: flex;
      gap: 1rem;
    }

    .card {
      display: flex;
      flex-direction: column;
      flex: 1;
      height: 20rem;
      box-shadow: 0 0 0.3rem gray;
      border-radius: 1rem;
      overflow: hidden;
      justify-content: center;
      align-items: center;
    }

    .card > .plot {
      flex: 1;
      height: 100%;
      width: auto;
      object-fit: contain;
    }

    .card > .title {
      box-sizing: border-box;
      flex-shrink: 0;
      padding: 0.5rem;
    }
  `;
}

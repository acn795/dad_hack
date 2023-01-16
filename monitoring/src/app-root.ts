import { LitElement, html, css, unsafeCSS, PropertyValueMap } from "lit";
import { customElement, state } from "lit/decorators.js";

import Icon from "./assets/favicon.png"

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
        ${this.renderCard("Weather", Icon)}
        ${this.renderCard("Fuel-Price", Icon)}
        ${this.renderCard("Bike-Count", Icon)}
      </div>

      <div class="subheader">"Fuel-Price" - "Bike-Count" correlations</div>
      <div class="row">
        ${this.renderCard("Diesel", Icon)}
        ${this.renderCard("E5", Icon)}
        ${this.renderCard("E10", Icon)}
      </div>

      <div class="subheader">"Weather" - "Bike-Count" correlations</div>
      <div class="row">
        ${this.renderCard("Rain", Icon)}
        ${this.renderCard("All", Icon)}
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

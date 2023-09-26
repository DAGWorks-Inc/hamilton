import React from 'react';
import clsx from 'clsx';
import styles from './styles.module.css';

const FeatureList = [
  {
    title: 'Easy to Use',
    Svg: require('@site/static/img/undraw_docusaurus_mountain.svg').default,
    description: (
      <>
        Hamilton was designed from the ground up to quickly and easily create
          dataflows. If you can draw a flow chart, you can create it in Hamilton.
          If you can read a python function you can understand a Hamilton dataflow.
      </>
    ),
  },
  {
    title: 'Focus on What Matters',
    Svg: require('@site/static/img/undraw_docusaurus_tree.svg').default,
    description: (
      <>
        Hamilton allows you to focus atomically on each step of your dataflow.
          Dataflows are also reusable and extensible so use this hub to help you
          find the code that you're looking for.
      </>
    ),
  },
  {
    title: 'Powered by Python',
    Svg: require('@site/static/img/undraw_docusaurus_react.svg').default,
    description: (
      <>
        Hamilton is built by defining python functions. You can do anything you can
          do in python in a Hamilton dataflow. You can also therefore run your dataflows
          anywhere you can run python.
      </>
    ),
  },
];

function Feature({Svg, title, description}) {
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center">
        <Svg className={styles.featureSvg} role="img" />
      </div>
      <div className="text--center padding-horiz--md">
        <h3>{title}</h3>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures() {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}

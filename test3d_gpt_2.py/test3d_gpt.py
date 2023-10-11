import sys
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QMainWindow, QVBoxLayout, QWidget
from PySide6.Qt3DExtras import Qt3DWindow, QEntity
from PySide6.Qt3DExtras import QOrbitCameraController
from PySide6.Qt3DRender import QCamera
from PySide6.QtGui import QVector3D
from PySide6.QtCharts import QtCharts

if __name__ == '__main__':
    # Create application
    app = QApplication(sys.argv)

    # Create main window
    main_window = QMainWindow()
    main_window.setWindowTitle("3D and 2D Layout")

    # Create central widget
    central_widget = QWidget()
    main_window.setCentralWidget(central_widget)

    # Create layout
    layout = QVBoxLayout(central_widget)

    # Create 3D window
    window3D = Qt3DWindow()
    window3D.defaultFrameGraph().setClearColor(Qt.black)
    container3D = QWidget.createWindowContainer(window3D)
    layout.addWidget(container3D)

    # Create 2D widget (Chart)
    chart = QtCharts.QChart()
    container2D = QtCharts.QChartView(chart)
    layout.addWidget(container2D)

    # Setup 3D scene
    scene = window3D.createDefaultWorld()
    camera_entity = scene.createEntity()
    camera = QCamera(camera_entity)
    camera.lens().setPerspectiveProjection(45.0, 16/9, 0.1, 1000)
    camera.setPosition(QVector3D(0, 0, 20))
    camera.setViewCenter(QVector3D(0, 0, 0))
    camera_entity.addComponent(camera)
    camera_controller = QOrbitCameraController(scene)
    camera_controller.setCamera(camera)
    window3D.setRootEntity(camera_entity)

    # Setup 2D chart
    series = QtCharts.QLineSeries()
    series << QPointF(0, 0) << QPointF(1, 1) << QPointF(2, 3) << QPointF(3, 2) << QPointF(4, 4)
    chart.addSeries(series)
    chart.createDefaultAxes()
    chart.setTitle("Sample Line Chart")

    # Show main window
    main_window.show()
    
    # Run the event loop
    sys.exit(app.exec())